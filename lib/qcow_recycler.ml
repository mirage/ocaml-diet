(* Securely erase and then recycle clusters *)

let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info);
  src

module Log = (val Logs.src_log src : Logs.LOG)

module Int64Map = Map.Make(Int64)

let ( <| ) = Int64.shift_left

type move = {
  move: Qcow_cluster_map.Move.t;
  state: Qcow_cluster_map.move_state;
}
(** describes the state of an in-progress block move *)

type clusters = {
  moves: move Int64Map.t;
  (** all in-progress block moves, indexed by the source cluster *)
}

let nothing = {
  moves = Int64Map.empty;
}

module Cache = Qcow_cache
module Metadata = Qcow_metadata

module Make(B: Qcow_s.RESIZABLE_BLOCK) = struct

  type t = {
    base: B.t;
    sector_size: int;
    cluster_bits: int;
    mutable cluster_map: Qcow_cluster_map.t option; (* free/ used space map *)
    cache: Cache.t;
    locks: Qcow_cluster.t;
    metadata: Metadata.t;
    mutable clusters: clusters;
    cluster: Cstruct.t; (* a zero cluster for erasing *)
    mutable background_thread: unit Lwt.t;
    m: Lwt_mutex.t;
  }

  let create ~base ~sector_size ~cluster_bits ~cache ~locks ~metadata =
    let clusters = nothing in
    let npages = 1 lsl (cluster_bits - 12) in
    let pages = Io_page.(to_cstruct @@ get npages) in
    let cluster = Cstruct.sub pages 0 (1 lsl cluster_bits) in
    Cstruct.memset cluster 0;
    let background_thread = Lwt.return_unit in
    let m = Lwt_mutex.create () in
    let cluster_map = None in
    { base; sector_size; cluster_bits; cluster_map; cache; locks; metadata;
      clusters; cluster; background_thread; m }

  let set_cluster_map t cluster_map = t.cluster_map <- Some cluster_map

  let start_background_thread t ~keep_erased =
    let th, _ = Lwt.task () in
    Lwt.on_cancel th
      (fun () ->
        Log.info (fun f -> f "cancellation of block recycler not implemented");
      );
    let cluster_map = match t.cluster_map with
      | Some x -> x
      | None -> assert false in
    Log.info (fun f -> f "block recycler starting with keep_erased = %Ld" keep_erased);
    let rec loop () =
      let open Lwt.Infix in
      Qcow_cluster_map.wait_for_junk cluster_map
      >>= fun () ->
      Log.info (fun f -> f "block recycler: %s" (Qcow_cluster_map.to_summary_string cluster_map));
      loop () in
    Lwt.async loop;
    t.background_thread <- th

  let allocate t n =
    let cluster_map = match t.cluster_map with
      | Some x -> x
      | None -> assert false in
    match Qcow_clusterset.take (Qcow_cluster_map.available cluster_map) n with
    | Some (set, _free) ->
      Log.debug (fun f -> f "Allocated %Ld clusters from free list" n);
      Qcow_cluster_map.remove_from_available cluster_map set;
      Some set
    | None ->
      None

  let copy t src dst =
    Qcow_cluster.with_read_lock t.locks src
      (fun () ->
         Qcow_cluster.with_write_lock t.locks dst
           (fun () ->
              Log.debug (fun f -> f "Copy cluster %Ld to %Ld" src dst);
              let npages = 1 lsl (t.cluster_bits - 12) in
              let pages = Io_page.(to_cstruct @@ get npages) in
              let cluster = Cstruct.sub pages 0 (1 lsl t.cluster_bits) in

              let sectors_per_cluster = Int64.(div (1L <| t.cluster_bits) (of_int t.sector_size)) in

              let src_sector = Int64.mul src sectors_per_cluster in
              let dst_sector = Int64.mul dst sectors_per_cluster in
              let open Lwt.Infix in
              B.read t.base src_sector [ cluster ]
              >>= function
              | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
              | Error `Disconnected -> Lwt.return (Error `Disconnected)
              | Ok () ->
                B.write t.base dst_sector [ cluster ]
                >>= function
                | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                | Error `Disconnected -> Lwt.return (Error `Disconnected)
                | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
                | Ok () ->
                  (* If these were metadata blocks (e.g. L2 table entries) then they might
                     be cached. Remove the overwritten block's cache entry just in case. *)
                  Cache.remove t.cache dst;
                  Lwt.return (Ok ())
           )
      )

  let move t move =
    let m = { move; state = Qcow_cluster_map.Copying } in
    let src, dst = Qcow_cluster_map.Move.(move.src, move.dst) in
    t.clusters <- { moves = Int64Map.add src m t.clusters.moves };
    let open Lwt.Infix in
    copy t src dst
    >>= function
    | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
    | Error `Disconnected -> Lwt.return (Error `Disconnected)
    | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
    | Ok () ->
      (* FIXME: make a concurrent write remove the entry *)
      t.clusters <- { moves =
                                        if Int64Map.mem src t.clusters.moves
                                        then Int64Map.add src { m with state = Qcow_cluster_map.Copied } t.clusters.moves
                                        else t.clusters.moves
                    };
      Lwt.return (Ok ())

  let erase t remaining =
    let open Lwt.Infix in
    let rec loop remaining =
      match Qcow_clusterset.min_elt remaining with
      | i ->
        let x, y = Qcow_clusterset.Interval.(x i, y i) in
        Log.debug (fun f -> f "erasing clusters (%Ld -> %Ld)" x y);
        let rec per_cluster x =
          if x > y
          then Lwt.return (Ok ())
          else begin
            let sector = Int64.(div (x <| t.cluster_bits) (of_int t.sector_size)) in
            Qcow_cluster.with_write_lock t.locks x
              (fun () ->
                 B.write t.base sector [ t.cluster ]
              )
            >>= function
            | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
            | Error `Disconnected -> Lwt.return (Error `Disconnected)
            | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
            | Ok () ->
              per_cluster (Int64.succ x)
          end in
        ( per_cluster x
          >>= function
          | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
          | Error `Disconnected -> Lwt.return (Error `Disconnected)
          | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
          | Ok () ->
            loop (Qcow_clusterset.remove i remaining) )
      | exception Not_found ->
        Lwt.return (Ok ()) in
    loop remaining

  let erase_all t =
    let cluster_map = match t.cluster_map with
      | Some x -> x
      | None -> assert false in
    let batch, _sizeof_batch = Qcow_cluster_map.junk cluster_map in
    Qcow_cluster_map.remove_from_junk cluster_map batch;
    let open Lwt.Infix in
    erase t batch
    >>= function
    | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
    | Error `Disconnected -> Lwt.return (Error `Disconnected)
    | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
    | Ok () ->
      Qcow_cluster_map.add_to_erased cluster_map batch;
      Lwt.return (Ok ())

  (* Run all threads in parallel, wait for all to complete, then iterate through
     the results and return the first failure we discover. *)
  let iter_p f xs =
    let threads = List.map f xs in
    Lwt_list.fold_left_s (fun acc t ->
        match acc with
        | Error x -> Lwt.return (Error x) (* first error wins *)
        | Ok () -> t
      ) (Ok ()) threads

  let update_references t =
    let open Qcow_cluster_map in
    let flushed =
      Int64Map.fold (fun _src move acc ->
        match move.state with
        | Flushed -> move :: acc
        | _ -> acc
      ) t.clusters.moves [] in
    let cluster_map = match t.cluster_map with
      | None -> assert false (* by construction, see `make` *)
      | Some x -> x in
    let nr_updated = ref 0L in
    let open Lwt.Infix in
    iter_p
      (fun ({ move = { Move.src; dst }; _ } as move) ->
        let ref_cluster, ref_cluster_within = match Qcow_cluster_map.find cluster_map src with
          | exception Not_found ->
            (* FIXME: block was probably discarded, but we'd like to avoid this case
               by construction *)
            Log.err (fun f -> f "Not_found reference to cluster %Ld (moving to %Ld)"
              src dst
            );
            assert false
          | a, b -> a, b in
        Qcow_cluster.with_metadata_lock t.locks
          (fun () ->
            Metadata.update t.metadata ref_cluster
              (fun c ->
                let addresses = Metadata.Physical.of_cluster c in
                (* Read the current value in the referencing cluster as a sanity check *)
                let old_reference = Metadata.Physical.get addresses ref_cluster_within in
                let old_cluster = Qcow_physical.cluster ~cluster_bits:t.cluster_bits old_reference in
                if old_cluster <> src then begin
                  Log.err (fun f -> f "Rewriting reference in %Ld :%d from %Ld to %Ld, old reference actually pointing to %Ld" ref_cluster ref_cluster_within src dst old_cluster);
                  assert false
                end;
                Log.debug (fun f -> f "Rewriting reference in %Ld :%d from %Ld to %Ld" ref_cluster ref_cluster_within src dst);
                (* Preserve any flags but update the pointer *)
                let new_reference = Qcow_physical.make ~is_mutable:(Qcow_physical.is_mutable old_reference) ~is_compressed:(Qcow_physical.is_compressed old_reference) (dst <| t.cluster_bits) in
                Metadata.Physical.set addresses ref_cluster_within new_reference;
                Lwt.return (Ok ())
              )
          )
        >>= function
        | Ok () ->
          if Int64Map.mem src t.clusters.moves
          then t.clusters <- { moves = Int64Map.add src { move with state = Referenced } t.clusters.moves };
          nr_updated := Int64.add !nr_updated (Int64.of_int (List.length flushed));
          Lwt.return (Ok ())
        | Error e -> Lwt.return (Error e)
      ) flushed
    >>= function
    | Ok () -> Lwt.return (Ok !nr_updated)
    | Error e -> Lwt.return (Error e)

  let flush t =
    let open Qcow_cluster_map in
    let cluster_map = match t.cluster_map with
      | None -> assert false (* by construction, see `make` *)
      | Some x -> x in
    (* Anything erased right now will become available *)
    let clusters = t.clusters in
    let open Lwt.Infix in
    let erased = Qcow_cluster_map.erased cluster_map in
    B.flush t.base
    >>= function
    | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
    | Error `Disconnected -> Lwt.return (Error `Disconnected)
    | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
    | Ok () ->
      (* Walk over the moves in the map from before the flush, and accumulate
         changes on the current state of moves. If a move started while we
         were flushing, then it should be preserved as-is until the next flush. *)
      let moves, junk = Int64Map.fold (fun src (move: move) (acc, junk) ->
          if not(Int64Map.mem src acc) then begin
            (* This move appeared while the flush was happening: next time *)
            acc, junk
          end else begin
            match move.state with
            | Copying ->
              acc, junk
            | Copied ->
              Int64Map.add src { move with state = Flushed } acc, junk
            | Flushed ->
              (* FIXME: who rewrites the references *)
              Int64Map.add src { move with state = Flushed } acc, junk
            | Referenced ->
              Int64Map.remove src acc, Qcow_clusterset.(add (Interval.make src src) junk)
          end
        ) clusters.moves (t.clusters.moves, Qcow_clusterset.empty) in
      Qcow_cluster_map.add_to_junk cluster_map junk;
      Qcow_cluster_map.add_to_available cluster_map erased;
      Qcow_cluster_map.remove_from_erased cluster_map erased;
      t.clusters <- {
        moves;
      };
      Lwt.return (Ok ())
end
