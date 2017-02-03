(* Securely erase and then recycle clusters *)

let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info);
  src

module Log = (val Logs.src_log src : Logs.LOG)

module Int64Map = Map.Make(Int64)

let ( <| ) = Int64.shift_left

type move_state =
  | Copying
  (** a background copy is in progress. If this cluster is modified then
      the copy should be aborted. *)
  | Copied
  (** contents of this cluster have been copied once to another cluster.
      If this cluster is modified then the copy should be aborted. *)
  | Flushed
  (** contents of this cluster have been copied and flushed to disk: it
      is now safe to rewrite the pointer. If this cluster is modified then
      the copy should be aborted. *)
  | Referenced
  (** the reference has been rewritten; it is now safe to write to this
      cluster again. On the next flush, the copy is complete and the original
      block can be recycled. *)

type move = {
  move: Qcow_cluster_map.Move.t;
  state: move_state;
}
(** describes the state of an in-progress block move *)

type clusters = {
  available: Qcow_clusterset.t;
  (** guaranteed to contain zeroes even after a crash *)

  erased: Qcow_clusterset.t;
  (** zeroed but not yet flushed so the old data may come back after a crash *)

  junk: Qcow_clusterset.t;
  (** unused clusters containing arbitrary data *)

  moves: move Int64Map.t;
  (** all in-progress block moves, indexed by the source cluster *)
}

let nothing = {
  available = Qcow_clusterset.empty;
  erased = Qcow_clusterset.empty;
  junk = Qcow_clusterset.empty;
  moves = Int64Map.empty;
}

module Cache = Qcow_cache

module Make(B: Qcow_s.RESIZABLE_BLOCK) = struct

  type t = {
    base: B.t;
    sector_size: int;
    cluster_bits: int;
    cache: Cache.t;
    locks: Qcow_cluster.t;
    mutable clusters: clusters;
    cluster: Cstruct.t; (* a zero cluster for erasing *)
    m: Lwt_mutex.t;
  }

  let create ~base ~sector_size ~cluster_bits ~cache ~locks =
    let clusters = nothing in
    let npages = 1 lsl (cluster_bits - 12) in
    let pages = Io_page.(to_cstruct @@ get npages) in
    let cluster = Cstruct.sub pages 0 (1 lsl cluster_bits) in
    Cstruct.memset cluster 0;
    let m = Lwt_mutex.create () in
    { base; sector_size; cluster_bits; cache; locks; clusters; cluster; m }

  (* Called after a full compact to reset everything. Otherwise we may try to
     erase blocks which nolonger exist. *)
  let reset t =
    t.clusters <- nothing

  let add_to_junk t cluster =
    let i = Qcow_clusterset.Interval.make cluster cluster in
    t.clusters <- { t.clusters with junk = Qcow_clusterset.add i t.clusters.junk }

  let allocate t n =
    match Qcow_clusterset.take t.clusters.available n with
    | Some (set, free) ->
      Log.debug (fun f -> f "Allocated %Ld clusters from free list" n);
      t.clusters <- { t.clusters with available = free };
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
    let m = { move; state = Copying } in
    let src, dst = Qcow_cluster_map.Move.(move.src, move.dst) in
    t.clusters <- { t.clusters with moves = Int64Map.add src m t.clusters.moves };
    let open Lwt.Infix in
    copy t src dst
    >>= function
    | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
    | Error `Disconnected -> Lwt.return (Error `Disconnected)
    | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
    | Ok () ->
      (* FIXME: make a concurrent write remove the entry *)
      t.clusters <- { t.clusters with moves =
                                        if Int64Map.mem src t.clusters.moves
                                        then Int64Map.add src { m with state = Copied } t.clusters.moves
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
    let batch = t.clusters.junk in
    t.clusters <- { t.clusters with junk = Qcow_clusterset.empty };
    let open Lwt.Infix in
    erase t batch
    >>= function
    | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
    | Error `Disconnected -> Lwt.return (Error `Disconnected)
    | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
    | Ok () ->
      t.clusters <- { t.clusters with erased = Qcow_clusterset.union batch t.clusters.erased };
      Lwt.return (Ok ())

  let flush t =
    (* Anything erased right now will become available *)
    let clusters = t.clusters in
    let open Lwt.Infix in
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
            | Copied ->
              Int64Map.add src { move with state = Flushed } acc, junk
            | Flushed ->
              (* FIXME: who rewrites the references *)
              Int64Map.add src { move with state = Flushed } acc, junk
            | Referenced ->
              Int64Map.remove src acc, Qcow_clusterset.(add (Interval.make src src) junk)
          end
        ) clusters.moves (t.clusters.moves, Qcow_clusterset.empty) in
      t.clusters <- {
        available = Qcow_clusterset.union t.clusters.available clusters.erased;
        erased = Qcow_clusterset.diff t.clusters.erased clusters.erased;
        junk = Qcow_clusterset.union t.clusters.junk junk;
        moves;
      };
      Lwt.return (Ok ())
end
