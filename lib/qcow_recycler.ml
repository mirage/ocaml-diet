(* Securely erase and then recycle clusters *)

let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info);
  src

module Log = (val Logs.src_log src : Logs.LOG)

open Qcow_types

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

module Make(B: Qcow_s.RESIZABLE_BLOCK)(Time: Mirage_time_lwt.S) = struct

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
    mutable need_to_flush: bool;
    need_to_flush_c: unit Lwt_condition.t;
    need_to_update_references_c: unit Lwt_condition.t;
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
    let need_to_flush = false in
    let need_to_flush_c = Lwt_condition.create () in
    let need_to_update_references_c = Lwt_condition.create () in
    { base; sector_size; cluster_bits; cluster_map; cache; locks; metadata;
      clusters; cluster; background_thread; need_to_flush; need_to_flush_c;
      need_to_update_references_c; m }

  let set_cluster_map t cluster_map = t.cluster_map <- Some cluster_map

  let allocate t n =
    let cluster_map = match t.cluster_map with
      | Some x -> x
      | None -> assert false in
    match Int64.IntervalSet.take (Qcow_cluster_map.available cluster_map) n with
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
      match Int64.IntervalSet.min_elt remaining with
      | i ->
        let x, y = Int64.IntervalSet.Interval.(x i, y i) in
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
            loop (Int64.IntervalSet.remove i remaining) )
      | exception Not_found ->
        Lwt.return (Ok ()) in
    loop remaining

  let erase_all t =
    let cluster_map = match t.cluster_map with
      | Some x -> x
      | None -> assert false in
    let batch = Qcow_cluster_map.junk cluster_map in
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
    | Ok () ->
      t.need_to_flush <- true;
      Lwt_condition.signal t.need_to_flush_c ();
      Lwt.return (Ok !nr_updated)
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
      let update_references = ref false in
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
              update_references := true;
              Int64Map.add src { move with state = Flushed } acc, junk
            | Flushed ->
              Int64Map.add src { move with state = Flushed } acc, junk
            | Referenced ->
              Int64Map.remove src acc, Int64.IntervalSet.(add (Interval.make src src) junk)
          end
        ) clusters.moves (t.clusters.moves, Int64.IntervalSet.empty) in
      Qcow_cluster_map.add_to_junk cluster_map junk;
      Qcow_cluster_map.add_to_available cluster_map erased;
      Qcow_cluster_map.remove_from_erased cluster_map erased;
      t.clusters <- {
        moves;
      };
      if !update_references then begin
        Lwt_condition.signal t.need_to_update_references_c ();
      end;
      Lwt.return (Ok ())

  let start_background_thread t ~keep_erased ?compact_after_unmaps () =
    let th, _ = Lwt.task () in
    Lwt.on_cancel th
      (fun () ->
        Log.info (fun f -> f "cancellation of block recycler not implemented");
      );
    let cluster_map = match t.cluster_map with
      | Some x -> x
      | None -> assert false in
    Log.info (fun f -> f "block recycler starting with keep_erased = %Ld" keep_erased);
    let open Lwt.Infix in

    let rec background_flusher () =
      let rec wait () = match t.need_to_flush with
        | true -> Lwt.return_unit
        | false ->
          Lwt_condition.wait t.need_to_flush_c
          >>= fun () ->
          wait () in
      wait ()
      >>= fun () ->
      t.need_to_flush <- false;
      Time.sleep_ns 5_000_000_000L
      >>= fun () ->
      Log.info (fun f -> f "block recycler triggering background flush");
      flush t
      >>= function
      | Error _ ->
        Log.err (fun f -> f "block recycler: flush failed");
        Lwt.return_unit
      | Ok () ->
        background_flusher () in
    Lwt.async background_flusher;

    let rec wait_for_work () =
      let junk = Qcow_cluster_map.junk cluster_map in
      let nr_junk = Int64.IntervalSet.cardinal junk in
      let erased = Qcow_cluster_map.erased cluster_map in
      let nr_erased = Int64.IntervalSet.cardinal erased in
      let available = Qcow_cluster_map.available cluster_map in
      let nr_available = Int64.IntervalSet.cardinal available in
      (* Apply the threshold to the total clusters erased, which includes those
         marked as available *)
      let total_erased = Int64.add nr_erased nr_available in
      (* Prioritise cluster reuse because it's more efficient not to have to
         move a cluster at all U*)
      let highest_priority =
        if total_erased < keep_erased && nr_junk > 0L then begin
          (* Take some of the junk and erase it *)
          let n = min nr_junk (Int64.sub keep_erased total_erased) in
          match Int64.IntervalSet.take junk n with
          | None -> None
          | Some (to_erase, _) -> Some (`Erase to_erase)
        end else None in
      (* If we need to update references, do that next *)
      let middle_priority =
        let flushed =
          Int64Map.fold (fun _src move acc ->
            match move.state with
            | Qcow_cluster_map.Flushed -> true
            | _ -> acc
          ) t.clusters.moves false in
        if flushed then Some `Update_references else None in
      let work = match highest_priority, middle_priority, compact_after_unmaps with
        | Some x, _, _ -> Some x
        | _, Some x, _ -> Some x
        | None, _, Some x when x < nr_junk -> Some (`Move nr_junk)
        | _ -> None in
      match work with
      | None ->
        Lwt.pick [ Qcow_cluster_map.wait cluster_map; Lwt_condition.wait t.need_to_update_references_c ]
        >>= fun () ->
        wait_for_work ()
      | Some work ->
        Lwt.return work in

    let rec loop () =
      t.need_to_flush <- true;
      Lwt_condition.signal t.need_to_flush_c (); (* trigger a flush later *)
      wait_for_work ()
      >>= function
      | `Erase to_erase ->
        Log.info (fun f -> f "block recycler: should erase %Ld clusters" (Int64.IntervalSet.cardinal to_erase));
        begin erase t to_erase
        >>= function
        | Error `Unimplemented -> Lwt.fail_with "Unimplemented"
        | Error `Disconnected -> Lwt.fail_with "Disconnected"
        | Error `Is_read_only -> Lwt.fail_with "Is_read_only"
        | Ok () ->
          Qcow_cluster_map.add_to_erased cluster_map to_erase;
          loop ()
        end
      | `Move nr_junk ->
        Log.info (fun f -> f "block recycler: should compact up to %Ld clusters" nr_junk);
        begin Qcow_cluster_map.compact_s
          (fun m () ->
            move t m
            >>= function
            | Error e -> Lwt.return (Error e)
            | Ok () -> Lwt.return (Ok (false, ()))
          ) cluster_map ()
          >>= function
          | Error `Unimplemented -> Lwt.fail_with "Unimplemented"
          | Error `Disconnected -> Lwt.fail_with "Disconnected"
          | Error `Is_read_only -> Lwt.fail_with "Is_read_only"
          | Ok () -> loop ()
        end
      | `Update_references ->
        Log.info (fun f -> f "block recycler: need to update references to blocks");
        update_references t
        >>= function
        | Error (`Msg x) -> Lwt.fail_with x
        | Error `Unimplemented -> Lwt.fail_with "Unimplemented"
        | Error `Disconnected -> Lwt.fail_with "Disconnected"
        | Error `Is_read_only -> Lwt.fail_with "Is_read_only"
        | Ok _nr_updated -> loop ()
      in

    Lwt.async loop;
    t.background_thread <- th
end
