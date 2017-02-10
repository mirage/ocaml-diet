(* Securely erase and then recycle clusters *)

let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info);
  src

module Log = (val Logs.src_log src : Logs.LOG)

open Qcow_types

let ( <| ) = Int64.shift_left
let ( |> ) = Int64.shift_right

module Cache = Qcow_cache
module Metadata = Qcow_metadata
module Physical = Qcow_physical

module Make(B: Qcow_s.RESIZABLE_BLOCK)(Time: Mirage_time_lwt.S) = struct

  type t = {
    base: B.t;
    sector_size: int;
    cluster_bits: int;
    mutable cluster_map: Qcow_cluster_map.t option; (* free/ used space map *)
    cache: Cache.t;
    locks: Qcow_cluster.t;
    metadata: Metadata.t;
    zero_buffer: Cstruct.t;
    mutable background_thread: unit Lwt.t;
    mutable need_to_flush: bool;
    need_to_flush_c: unit Lwt_condition.t;
    m: Lwt_mutex.t;
  }

  let create ~base ~sector_size ~cluster_bits ~cache ~locks ~metadata =
    let zero_buffer = Io_page.(to_cstruct @@ get 256) in (* 1 MiB *)
    Cstruct.memset zero_buffer 0;
    let background_thread = Lwt.return_unit in
    let m = Lwt_mutex.create () in
    let cluster_map = None in
    let need_to_flush = false in
    let need_to_flush_c = Lwt_condition.create () in
    { base; sector_size; cluster_bits; cluster_map; cache; locks; metadata;
      zero_buffer; background_thread; need_to_flush; need_to_flush_c;
      m }

  let set_cluster_map t cluster_map = t.cluster_map <- Some cluster_map

  let allocate t n =
    let cluster_map = match t.cluster_map with
      | Some x -> x
      | None -> assert false in
    match Int64.IntervalSet.take (Qcow_cluster_map.Available.get cluster_map) n with
    | Some (set, _free) ->
      Log.debug (fun f -> f "Allocated %Ld clusters from free list" n);
      Qcow_cluster_map.Available.remove cluster_map set;
      Some set
    | None ->
      None

  let copy_already_locked t src dst =
    let cluster_map = match t.cluster_map with
      | Some x -> x
      | None -> assert false in
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
        Cache.Debug.assert_not_cached t.cache dst;
        (* If the destination block was being moved, abort the move since the
           original copy has diverged. *)
        Qcow_cluster_map.cancel_move cluster_map dst;
        Lwt.return (Ok ())

  let copy t src dst =
    Qcow_cluster.with_read_lock t.locks src
      (fun () ->
         Qcow_cluster.with_write_lock t.locks dst
           (fun () ->
             copy_already_locked t src dst
           )
      )

  let move t move =
    let cluster_map = match t.cluster_map with
      | Some x -> x
      | None -> assert false in
    Qcow_cluster_map.(set_move_state cluster_map move Copying);
    let src, dst = Qcow_cluster_map.Move.(move.src, move.dst) in
    let open Lwt.Infix in
    Qcow_cluster.with_read_lock t.locks src
      (fun () ->
         Qcow_cluster.with_write_lock t.locks dst
           (fun () ->
             copy_already_locked t src dst
             >>= function
             | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
             | Error `Disconnected -> Lwt.return (Error `Disconnected)
             | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
             | Ok () ->
               Qcow_cluster_map.(set_move_state cluster_map move Copied);
               Lwt.return (Ok ())
            )
      )

  let erase t remaining =
    let open Lwt.Infix in
    let intervals = Int64.IntervalSet.fold (fun i acc -> i :: acc) remaining [] in
    let buffer_size_clusters = Int64.of_int (Cstruct.len t.zero_buffer) |> t.cluster_bits in
    (* If any is an error, return it *)
    let rec any_error = function
      | [] -> Ok ()
      | (Error e) :: _ -> Error e
      | _ :: rest -> any_error rest in
    Lwt_list.map_p
      (fun i ->
        let x, y = Int64.IntervalSet.Interval.(x i, y i) in
        let n = Int64.(succ @@ sub y x) in
        Log.debug (fun f -> f "erasing %Ld clusters (%Ld -> %Ld)" n x y);
        let erase cluster n =
          (* Erase [n] clusters starting from [cluster] *)
          assert (n <= buffer_size_clusters);
          let buf = Cstruct.sub t.zero_buffer 0 (Int64.to_int (n <| t.cluster_bits)) in
          let sector = Int64.(div (cluster <| t.cluster_bits) (of_int t.sector_size)) in
          (* No-one else is writing to this cluster so no locking is needed *)
          B.write t.base sector [ buf ] in
        let rec chop_into from n m =
          if n = 0L then []
          else if n > m then (from, m) :: (chop_into (Int64.add from m) (Int64.sub n m) m)
          else [ from, n ] in
        Lwt_list.map_p (fun (cluster, n) -> erase cluster n) (chop_into x n buffer_size_clusters)
        >>= fun results ->
        Lwt.return (any_error results)
      ) intervals
    >>= fun results ->
    Lwt.return (any_error results)

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
    let cluster_map = match t.cluster_map with
      | None -> assert false (* by construction, see `make` *)
      | Some x -> x in
    let open Qcow_cluster_map in
    Qcow_cluster.with_metadata_lock t.locks
      (fun () ->

    let flushed =
      Int64.Map.fold (fun _src move acc ->
        match move.state with
        | Flushed -> move :: acc
        | _ -> acc
      ) (moves cluster_map) [] in
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

            Metadata.update t.metadata ref_cluster
              (fun c ->
                if not(Int64.Map.mem src (moves cluster_map)) then begin
                  Log.warn (fun f -> f "Not rewriting reference in %Ld :%d from %Ld to %Ld: move as been cancelled" ref_cluster ref_cluster_within src dst);
                  Lwt.return (Ok ())
                end else begin
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
                end
              )
        >>= function
        | Ok () ->
          set_move_state cluster_map move.move Referenced;
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
  )

  let flush t =
    let open Qcow_cluster_map in
    let cluster_map = match t.cluster_map with
      | None -> assert false (* by construction, see `make` *)
      | Some x -> x in
    let open Lwt.Infix in
    (* Anything erased right now will become available *)
    let erased = Qcow_cluster_map.Erased.get cluster_map in
    let moves = Qcow_cluster_map.moves cluster_map in
    B.flush t.base
    >>= function
    | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
    | Error `Disconnected -> Lwt.return (Error `Disconnected)
    | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
    | Ok () ->
      (* Walk over the snapshot of moves before the flush and update. This
         ensures we don't accidentally advance the state of moves which appeared
         after the flush. *)
      let junk = Int64.Map.fold (fun src (move: move) junk ->
        match move.state with
        | Copying ->
          junk
        | Copied | Flushed ->
          Qcow_cluster_map.(set_move_state cluster_map move.move Flushed);
          junk
        | Referenced ->
          Qcow_cluster_map.complete_move cluster_map move.move;
          Int64.IntervalSet.(add (Interval.make src src) junk)
        ) moves Int64.IntervalSet.empty in
      Qcow_cluster_map.Junk.add cluster_map junk;
      Qcow_cluster_map.Available.add cluster_map erased;
      Qcow_cluster_map.Erased.remove cluster_map erased;
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
      Log.info (fun f -> f "block recycler: triggering background flush: %s" (Qcow_cluster_map.to_summary_string cluster_map));
      flush t
      >>= function
      | Error _ ->
        Log.err (fun f -> f "block recycler: flush failed");
        Lwt.return_unit
      | Ok () ->
        background_flusher () in
    Lwt.async background_flusher;

    let last_block = ref (Qcow_cluster_map.get_last_block cluster_map) in
    let rec wait_for_work () =
      let junk = Qcow_cluster_map.Junk.get cluster_map in
      let nr_junk = Int64.IntervalSet.cardinal junk in
      let erased = Qcow_cluster_map.Erased.get cluster_map in
      let nr_erased = Int64.IntervalSet.cardinal erased in
      let available = Qcow_cluster_map.Available.get cluster_map in
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
          Int64.Map.fold (fun _src move acc ->
            match move.Qcow_cluster_map.state with
            | Qcow_cluster_map.Flushed -> true
            | _ -> acc
          ) (Qcow_cluster_map.moves cluster_map) false in
        if flushed then Some `Update_references else None in
      let work = match highest_priority, middle_priority, compact_after_unmaps with
        | Some x, _, _ -> Some x
        | _, Some x, _ -> Some x
        | None, _, Some x when x < nr_junk -> Some (`Move nr_junk)
        | _ ->
          let last_block' = Qcow_cluster_map.get_last_block cluster_map in
          let result =
            if last_block' < !last_block then Some `Resize else None in
          last_block := last_block';
          result in
      match work with
      | None ->
        Qcow_cluster_map.wait cluster_map
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
        Log.debug (fun f -> f "block recycler: should erase %Ld clusters" (Int64.IntervalSet.cardinal to_erase));
        begin erase t to_erase
        >>= function
        | Error `Unimplemented -> Lwt.fail_with "Unimplemented"
        | Error `Disconnected -> Lwt.fail_with "Disconnected"
        | Error `Is_read_only -> Lwt.fail_with "Is_read_only"
        | Ok () ->
          Qcow_cluster_map.Junk.remove cluster_map to_erase;
          Qcow_cluster_map.Erased.add cluster_map to_erase;
          loop ()
        end
      | `Move nr_junk ->
        Log.debug (fun f -> f "block recycler: should compact up to %Ld clusters" nr_junk);
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
        Log.debug (fun f -> f "block recycler: need to update references to blocks");
        begin update_references t
          >>= function
          | Error (`Msg x) -> Lwt.fail_with x
          | Error `Unimplemented -> Lwt.fail_with "Unimplemented"
          | Error `Disconnected -> Lwt.fail_with "Disconnected"
          | Error `Is_read_only -> Lwt.fail_with "Is_read_only"
          | Ok _nr_updated -> loop ()
        end
      | `Resize ->
        Qcow_cluster.with_metadata_lock t.locks
          (fun () ->
            let new_last_block = Qcow_cluster_map.get_last_block cluster_map in
            Log.debug (fun f -> f "block recycler: resize for last_block = %Ld" new_last_block);
            let new_size = Physical.make (Int64.succ new_last_block <| t.cluster_bits) in
            let sector = Physical.sector ~sector_size:t.sector_size new_size in
            let cluster = Physical.cluster ~cluster_bits:t.cluster_bits new_size in
            Qcow_cluster_map.resize cluster_map cluster;
            B.resize t.base sector
            >>= function
            | Error _ -> Lwt.fail_with "resize"
            | Ok () ->
            Log.debug (fun f -> f "Resized device to %Ld sectors of size %d" (Qcow_physical.to_bytes new_size) t.sector_size);
            Lwt.return_unit
          ) >>= fun () ->
          loop ()
      in

    Lwt.async loop;
    t.background_thread <- th
end
