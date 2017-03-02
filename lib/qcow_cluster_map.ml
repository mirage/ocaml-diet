(*
 * Copyright (C) 2016 David Scott <dave@recoil.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *)

let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info);
  src

module Log = (val Logs.src_log src : Logs.LOG)

open Qcow_types
module Cache = Qcow_cache

type reference = Cluster.t * int

type move_state =
  | Copying
  | Copied
  | Flushed
  | Referenced

let string_of_move_state = function
  | Copying    -> "Copying"
  | Copied     -> "Copied"
  | Flushed    -> "Flushed"
  | Referenced -> "Referenced"

module Move = struct
  type t = { src: Cluster.t; dst: Cluster.t }

  let to_string t =
    Printf.sprintf "%s -> %s" (Cluster.to_string t.src) (Cluster.to_string t.dst)
end

type move = {
  move: Move.t;
  state: move_state;
}

let string_of_move m =
  let state = string_of_move_state m.state in
  Printf.sprintf "%s %s"
    (Move.to_string m.move)
    state

type t = {
  mutable junk: Cluster.IntervalSet.t;
  (** These are unused clusters containing arbitrary data. They must be erased
      or fully overwritten and then flushed in order to be safely reused. *)
  mutable erased: Cluster.IntervalSet.t;
  (* These are clusters which have been erased, but not flushed. They will become
     available for reallocation on the next flush. *)
  mutable available: Cluster.IntervalSet.t;
  (** These clusters are available for immediate reuse; after a crash they are
      guaranteed to be full of zeroes. *)
  mutable roots: Cluster.IntervalSet.t;
  (* map from physical cluster to the physical cluster + offset of the reference.
     When a block is moved, this reference must be updated. *)
  mutable copies: Cluster.IntervalSet.t;
  (** Clusters which contain clusters as part of a move *)
  mutable moves: move Cluster.Map.t;
  (** The state of in-progress block moves, indexed by the source cluster *)
  mutable refs: reference Cluster.Map.t;
  first_movable_cluster: Cluster.t;
  cache: Cache.t;
  c: unit Lwt_condition.t;
  (** Signalled when any of the junk/erased sets change or when references need
      to be rewritten to kick the background recycling thread. *)
  runtime_asserts: bool;
  (** Check leak and sharing invariants on every update *)
}

let get_last_block t =
  let max_ref =
    try
      fst @@ Cluster.Map.max_binding t.refs
    with Not_found ->
      Cluster.pred t.first_movable_cluster in
  let max_root =
    try
      Cluster.IntervalSet.Interval.y @@ Cluster.IntervalSet.max_elt t.roots
    with Not_found ->
      max_ref in
  let max_copies =
    try
      Cluster.IntervalSet.Interval.y @@ Cluster.IntervalSet.max_elt t.copies
    with Not_found ->
      max_root in
  max (Cluster.pred t.first_movable_cluster) @@ max max_ref @@ max max_root max_copies

let total_used t =
  Int64.of_int @@ Cluster.Map.cardinal t.refs

let total_free t =
  Cluster.to_int64 @@ Cluster.IntervalSet.cardinal t.junk

let total_erased t =
  Cluster.to_int64 @@ Cluster.IntervalSet.cardinal t.erased

let total_available t =
  Cluster.to_int64 @@ Cluster.IntervalSet.cardinal t.available

let total_moves t =
  Cluster.Map.fold (fun _ m (copying, copied, flushed, referenced) -> match m.state with
    | Copying -> (copying + 1), copied, flushed, referenced
    | Copied -> copying, copied + 1, flushed, referenced
    | Flushed -> copying, copied, flushed + 1, referenced
    | Referenced -> copying, copied, flushed, referenced + 1
  ) t.moves (0, 0, 0, 0)

let total_copies t =
  Cluster.to_int64 @@ Cluster.IntervalSet.cardinal t.copies

let total_roots t =
  Cluster.to_int64 @@ Cluster.IntervalSet.cardinal t.roots

let to_summary_string t =
  let copying, copied, flushed, referenced = total_moves t in
  Printf.sprintf "%Ld used; %Ld junk; %Ld erased; %Ld available; %Ld copies; %Ld roots; %d Copying; %d Copied; %d Flushed; %d Referenced; max_cluster = %s"
    (total_used t) (total_free t) (total_erased t) (total_available t) (total_copies t) (total_roots t)
    copying copied flushed referenced (Cluster.to_string @@ get_last_block t)

module Debug = struct
  let check ?(leaks=true) ?(sharing=true) t =
    let open Cluster.IntervalSet in
    let last = get_last_block t in
    if last >= t.first_movable_cluster then begin
      let whole_file = add (Interval.make t.first_movable_cluster last) empty in
      let refs = Cluster.Map.fold (fun cluster _ set ->
        add (Interval.make cluster cluster) set
      ) t.refs empty in
      let moves = Cluster.Map.fold (fun _ m set ->
        let dst = m.move.Move.dst in
        add (Interval.make dst dst) set
      ) t.moves empty in
      let junk = "junk", t.junk in
      let erased = "erased", t.erased in
      let available = "available", t.available in
      let refs = "refs", refs in
      let moves = "moves", moves in
      let copies = "copies", t.copies in
      let roots = "roots", t.roots in
      let cached = "cached", Cache.Debug.all_cached_clusters t.cache in
      let all = [ junk; erased; available; refs; moves; copies; roots ] in
      let leaked = List.fold_left diff whole_file (List.map snd all) in
      if leaks && (cardinal leaked <> Cluster.zero) then begin
        Log.err (fun f -> f "%s" (to_summary_string t));
        Log.err (fun f -> f "%s clusters leaked: %s" (Cluster.to_string @@ cardinal leaked)
          (Sexplib.Sexp.to_string_hum (sexp_of_t leaked)));
        assert false
      end;
      let rec cross xs = function
        | [] -> []
        | y :: ys -> List.map (fun x -> x, y) xs @ cross xs ys in
      let check zs =
        List.iter (fun ((x_name, x), (y_name, y)) ->
          if x_name <> y_name then begin
            let i = inter x y in
            if cardinal i <> Cluster.zero then begin
              Log.err (fun f -> f "%s" (to_summary_string t));
              Log.err (fun f -> f "%s and %s are not disjoint" x_name y_name);
              Log.err (fun f -> f "%s = %s" x_name (Sexplib.Sexp.to_string_hum (sexp_of_t x)));
              Log.err (fun f -> f "%s = %s" y_name (Sexplib.Sexp.to_string_hum (sexp_of_t y)));
              Log.err (fun f -> f "intersection = %s" (Sexplib.Sexp.to_string_hum (sexp_of_t i)));
              assert false
            end
          end
        ) zs in
      (* These must be disjoint *)
      if sharing then begin
        check @@ cross
          [ junk; copies; erased; available; refs ]
          [ junk; copies; erased; available; refs ];
        check @@ cross
          [ cached ]
          [ junk; erased; available ]
      end
    end
    let assert_no_leaked_blocks t = check t
end

module type MutableSet = sig
  val get: t -> Cluster.IntervalSet.t
  val add: t -> Cluster.IntervalSet.t -> unit
  val remove: t -> Cluster.IntervalSet.t -> unit
end

let make ~free ~refs ~cache ~first_movable_cluster ~runtime_asserts =
  let junk = Qcow_bitmap.fold
    (fun i acc ->
      let x, y = Qcow_bitmap.Interval.(x i, y i) in
      let x = Cluster.of_int64 x and y = Cluster.of_int64 y in
      Cluster.IntervalSet.(add (Interval.make x y) acc)
    ) free Cluster.IntervalSet.empty in
  let copies = Cluster.IntervalSet.empty in
  let roots = Cluster.IntervalSet.empty in
  let available = Cluster.IntervalSet.empty in
  let erased = Cluster.IntervalSet.empty in
  let moves = Cluster.Map.empty in
  let c = Lwt_condition.create () in
  { junk; available; erased; copies; roots; moves; refs; first_movable_cluster; cache; c; runtime_asserts }

let zero =
  let free = Qcow_bitmap.make_empty ~initial_size:0 ~maximum_size:0 in
  let refs = Cluster.Map.empty in
  let cache = Cache.create
    ~read_cluster:(fun _ -> Lwt.return (Error `Unimplemented))
    ~write_cluster:(fun _ _ -> Lwt.return (Error `Unimplemented))
    () in
  make ~free ~refs ~first_movable_cluster:Cluster.zero ~cache ~runtime_asserts:false

let resize t new_size_clusters =
  let file = Cluster.IntervalSet.(add (Interval.make Cluster.zero (Cluster.pred new_size_clusters)) empty) in
  t.junk <- Cluster.IntervalSet.inter t.junk file;
  t.erased <- Cluster.IntervalSet.inter t.erased file;
  t.available <- Cluster.IntervalSet.inter t.available file

module Junk = struct
  let get t = t.junk
  let add t more =
    Log.debug (fun f -> f "Junk.add %s" (Sexplib.Sexp.to_string (Cluster.IntervalSet.sexp_of_t more)));
    t.junk <- Cluster.IntervalSet.union t.junk more;
    (* Ensure all cached copies of junk blocks are dropped *)
    Cluster.IntervalSet.(fold (fun i () ->
      let x, y = Interval.(x i, y i) in
      let rec loop n =
        if n <= y then begin
          Cache.remove t.cache n;
          loop (Cluster.succ n)
        end in
      loop x
    ) more ());
    if t.runtime_asserts then Debug.check ~leaks:false t;
    Lwt_condition.signal t.c ()
  let remove t less =
    t.junk <- Cluster.IntervalSet.diff t.junk less;
    Lwt_condition.signal t.c ()
end

module Available = struct
  let get t = t.available
  let add t more =
    Log.debug (fun f -> f "Available.add %s" (Sexplib.Sexp.to_string (Cluster.IntervalSet.sexp_of_t more)));
    t.available <- Cluster.IntervalSet.union t.available more;
    if t.runtime_asserts then Debug.check ~leaks:false t;
    Lwt_condition.signal t.c ()
  let remove t less =
    t.available <- Cluster.IntervalSet.diff t.available less;
    Lwt_condition.signal t.c ()
end

module Erased = struct
  let get t = t.erased
  let add t more =
    Log.debug (fun f -> f "Erased.add %s" (Sexplib.Sexp.to_string (Cluster.IntervalSet.sexp_of_t more)));
    t.erased <- Cluster.IntervalSet.union t.erased more;
    if t.runtime_asserts then Debug.check ~leaks:false t;
    Lwt_condition.signal t.c ()
  let remove t less =
    t.erased <- Cluster.IntervalSet.diff t.erased less;
    Lwt_condition.signal t.c ()
end

module Copies = struct
  let get t = t.erased
  let add t more =
    Log.debug (fun f -> f "Copies.add %s" (Sexplib.Sexp.to_string (Cluster.IntervalSet.sexp_of_t more)));
    t.copies <- Cluster.IntervalSet.union t.copies more;
    if t.runtime_asserts then Debug.check ~leaks:false t;
    Lwt_condition.signal t.c ()
  let remove t less =
    t.copies <- Cluster.IntervalSet.diff t.copies less;
    Lwt_condition.signal t.c ()
end


let wait t = Lwt_condition.wait t.c

let find t cluster = Cluster.Map.find cluster t.refs

let moves t = t.moves

let set_move_state t move state =
  let m = { move; state } in
  let old_state =
    if Cluster.Map.mem move.Move.src t.moves
    then Some ((Cluster.Map.find move.Move.src t.moves).state)
    else None in
  match old_state, state with
  | None, Copying ->
    let dst = move.Move.dst in
    let dst' = Cluster.IntervalSet.(add (Interval.make dst dst) empty) in
    (* We always move into junk blocks *)
    Junk.remove t dst';
    Log.debug (fun f -> f "Cluster %s None -> Copying" (Cluster.to_string move.Move.src));
    t.moves <- Cluster.Map.add move.Move.src m t.moves;
    Copies.add t dst'
  | Some Copied, Flushed ->
    Log.debug (fun f -> f "Cluster %s Copied -> Flushed" (Cluster.to_string move.Move.src));
    t.moves <- Cluster.Map.add move.Move.src m t.moves;
    (* References now need to be rewritten *)
    Lwt_condition.signal t.c ();
  | Some old, _ ->
    Log.debug (fun f -> f "Cluster %s %s -> %s" (Cluster.to_string move.Move.src)
      (string_of_move_state old) (string_of_move_state state));
    t.moves <- Cluster.Map.add move.Move.src m t.moves
  | None, _ ->
    Log.warn (fun f -> f "Not updating move state of cluster %s: operation cancelled" (Cluster.to_string move.Move.src))

let cancel_move t cluster =
  match Cluster.Map.find cluster t.moves with
    | { state = Referenced; _ } ->
      (* The write will have followed the reference to the destination block.
         There are 2 interesting possibilities if we crash without flushing:
         - neither the write nor the reference are committed: this behaves as if
           the write wasn't committed which is valid
         - the write is committed but the reference isn't: this also behaves
           as if the write wasn't committed which is valid
         The only reason we still track this move is because when the next flush
         happens it is safe to add the src cluster to the set of junk blocks. *)
      Log.info (fun f -> f "Not cancelling in-progress move of cluster %s: already Referenced" (Cluster.to_string cluster))
    | { move = { Move.dst; _ }; _ } ->
      Log.debug (fun f -> f "Cancelling in-progress move of cluster %s to %s" (Cluster.to_string cluster) (Cluster.to_string dst));
      t.moves <- Cluster.Map.remove cluster t.moves;
      let dst' = Cluster.IntervalSet.(add (Interval.make dst dst) empty) in
      (* The destination block can now be recycled *)
      Copies.remove t dst';
      Junk.add t dst'
    | exception Not_found ->
      ()

let complete_move t move =
  if not(Cluster.Map.mem move.Move.src t.moves)
  then Log.warn (fun f -> f "Not completing move state of cluster %s: operation cancelled" (Cluster.to_string move.Move.src))
  else begin
    t.moves <- Cluster.Map.remove move.Move.src t.moves;
    let dst = Cluster.IntervalSet.(add (Interval.make move.Move.dst move.Move.dst) empty) in
    Copies.remove t dst;
    let src = Cluster.IntervalSet.(add (Interval.make move.Move.src move.Move.src) empty) in
    Junk.add t src;
  end

let add t rf cluster =
  let c, w = rf in
  if cluster = Cluster.zero then () else begin
    if Cluster.Map.mem cluster t.refs then begin
      let c', w' = Cluster.Map.find cluster t.refs in
      Log.err (fun f -> f "Found two references to cluster %s: %s.%d and %s.%d" (Cluster.to_string cluster) (Cluster.to_string c) w (Cluster.to_string c') w');
      failwith (Printf.sprintf "Found two references to cluster %s: %s.%d and %s.%d" (Cluster.to_string cluster) (Cluster.to_string c) w (Cluster.to_string c') w');
    end;
    t.junk <- Cluster.IntervalSet.(remove (Interval.make cluster cluster) t.junk);
    t.refs <- Cluster.Map.add cluster rf t.refs;
    t.copies <- Cluster.IntervalSet.(remove (Interval.make cluster cluster) t.copies);
    ()
  end

let remove t cluster =
  t.junk <- Cluster.IntervalSet.(add (Interval.make cluster cluster) t.junk);
  t.refs <- Cluster.Map.remove cluster t.refs;
  Lwt_condition.signal t.c ()

let with_roots t clusters f =
  t.roots <- Cluster.IntervalSet.union clusters t.roots;
  Lwt.finalize f (fun () ->
    t.roots <- Cluster.IntervalSet.diff t.roots clusters;
    Lwt_condition.signal t.c ();
    Lwt.return_unit
  )

let get_moves t =
  (* The last allocated block. Note if there are no data blocks this will
     point to the last header block even though it is immovable. *)
  let max_cluster = get_last_block t in
  let refs = ref t.refs in
  fst @@ Cluster.IntervalSet.fold_individual
    (fun cluster (moves, max_cluster) ->
      (* A free block after the last allocated block will not be filled.
         It will be erased from existence when the file is truncated at the
         end. *)
      if cluster >= max_cluster then (moves, max_cluster) else begin
        (* find the last physical block *)
        let last_block, rf = Cluster.Map.max_binding (!refs) in

        if cluster >= last_block then moves, last_block else begin
          let src = last_block and dst = cluster in
          if Cluster.Map.mem src t.moves
          then moves, last_block (* move already in progress, don't move it again *)
          else begin
            (* copy last_block into cluster and update rf *)
            let move = { Move.src; dst } in
            refs := Cluster.Map.remove last_block @@ Cluster.Map.add cluster rf (!refs);
            move :: moves, last_block
          end
        end
      end
    ) t.junk ([], max_cluster)

let is_immovable t cluster = cluster < t.first_movable_cluster

let apply_moves t moves =
  let substitutions = List.fold_left (fun acc { Move.src; dst } ->
    Cluster.Map.add src dst acc
  ) Cluster.Map.empty moves in
  let refs = Cluster.Map.fold (fun to_c (from_c, from_w) acc ->
    (* Has the cluster [from_c] been moved? *)
    let from_c' = try Cluster.Map.find from_c substitutions with Not_found -> from_c in
    if from_c <> from_c' then begin
      Log.debug (fun f -> f "Updating reference %s:%d -> %s to %s:%d -> %s"
        (Cluster.to_string from_c) from_w (Cluster.to_string to_c)
        (Cluster.to_string from_c') from_w (Cluster.to_string to_c)
      );
    end;
    Cluster.Map.add to_c (from_c', from_w) acc
  ) t.refs Cluster.Map.empty in
  t.refs <- refs
