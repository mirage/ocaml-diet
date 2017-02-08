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
module ClusterMap = Int64.Map

type cluster = int64
type reference = cluster * int

type move_state =
  | Copying
  | Copied
  | Flushed
  | Referenced

module Move = struct
  type t = { src: cluster; dst: cluster }
end

type move = {
  move: Move.t;
  state: move_state;
}

type t = {
  mutable junk: Int64.IntervalSet.t;
  (** These are unused clusters containing arbitrary data. They must be erased
      or fully overwritten and then flushed in order to be safely reused. *)
  mutable erased: Int64.IntervalSet.t;
  (* These are clusters which have been erased, but not flushed. They will become
     available for reallocation on the next flush. *)
  mutable available: Int64.IntervalSet.t;
  (** These clusters are available for immediate reuse; after a crash they are
      guaranteed to be full of zeroes. *)
  mutable roots: Int64.IntervalSet.t;
  (* map from physical cluster to the physical cluster + offset of the reference.
     When a block is moved, this reference must be updated. *)
  mutable moves: move ClusterMap.t;
  (** The state of in-progress block moves, indexed by the source cluster *)
  mutable refs: reference ClusterMap.t;
  first_movable_cluster: int64;
  c: unit Lwt_condition.t;
  (** Signalled when any of the junk/erased sets change or when references need
      to be rewritten to kick the background recycling thread. *)
}

let make ~free ~refs ~first_movable_cluster =
  let junk = Qcow_bitmap.fold
    (fun i acc ->
      let x, y = Qcow_bitmap.Interval.(x i, y i) in
      Int64.IntervalSet.(add (Interval.make x y) acc)
    ) free Int64.IntervalSet.empty in
  let roots = Int64.IntervalSet.empty in
  let available = Int64.IntervalSet.empty in
  let erased = Int64.IntervalSet.empty in
  let moves = ClusterMap.empty in
  let c = Lwt_condition.create () in
  { junk; available; erased; roots; moves; refs; first_movable_cluster; c }

let zero =
  let free = Qcow_bitmap.make_empty ~initial_size:0 ~maximum_size:0 in
  let refs = ClusterMap.empty in
  make ~free ~refs ~first_movable_cluster:0L

let resize t new_size_clusters =
  let file = Int64.IntervalSet.(add (Interval.make 0L (Int64.pred new_size_clusters)) empty) in
  t.junk <- Int64.IntervalSet.inter t.junk file;
  t.erased <- Int64.IntervalSet.inter t.erased file;
  t.available <- Int64.IntervalSet.inter t.available file

let junk t = t.junk

let add_to_junk t more =
  (* assert (Int64.IntervalSet.inter t.junk more = Int64.IntervalSet.empty); *)
  t.junk <- Int64.IntervalSet.union t.junk more;
  Lwt_condition.signal t.c ()

let remove_from_junk t less =
  t.junk <- Int64.IntervalSet.diff t.junk less;
  Lwt_condition.signal t.c ()

let wait t = Lwt_condition.wait t.c

let available t = t.available

let add_to_available t more =
  t.available <- Int64.IntervalSet.union t.available more;
  Lwt_condition.signal t.c ()

let remove_from_available t less =
  t.available <- Int64.IntervalSet.diff t.available less;
  Lwt_condition.signal t.c ()

let erased t = t.erased

let add_to_erased t more =
  t.erased <- Int64.IntervalSet.union t.erased more;
  Lwt_condition.signal t.c ()

let remove_from_erased t less =
  t.erased <- Int64.IntervalSet.diff t.erased less;
  Lwt_condition.signal t.c ()

let find t cluster = ClusterMap.find cluster t.refs

let total_used t =
  Int64.of_int @@ ClusterMap.cardinal t.refs

let total_free t =
  Int64.IntervalSet.cardinal t.junk

let total_roots t =
  Int64.IntervalSet.cardinal t.roots

let moves t = t.moves

let set_move_state t move state =
  let m = { move; state } in
  let old_state =
    if ClusterMap.mem move.Move.src t.moves
    then Some ((ClusterMap.find move.Move.src t.moves).state)
    else None in
  match old_state, state with
  | None, Copying ->
    t.moves <- ClusterMap.add move.Move.src m t.moves
  | Some Copied, Flushed ->
    t.moves <- ClusterMap.add move.Move.src m t.moves;
    (* References now need to be rewritten *)
    Lwt_condition.signal t.c ();
  | Some _, _ ->
    t.moves <- ClusterMap.add move.Move.src m t.moves
  | None, _ ->
    Log.warn (fun f -> f "Not updating move state of cluster %Ld: operation cancelled" move.Move.src)

let cancel_move t cluster =
  match ClusterMap.find cluster t.moves with
    | { state = Referenced; _ } ->
      (* The write will have followed the reference to the destination block.
         There are 2 interesting possibilities if we crash without flushing:
         - neither the write nor the reference are committed: this behaves as if
           the write wasn't committed which is valid
         - the write is committed but the reference isn't: this also behaves
           as if the write wasn't committed which is valid
         The only reason we still track this move is because when the next flush
         happens it is safe to add the src cluster to the set of junk blocks. *)
      Log.debug (fun f -> f "Not cancelling in-progress move of cluter %Ld: already Referenced" cluster)
    | _ ->
      Log.warn (fun f -> f "Cancelling in-progress move of cluster %Ld" cluster);
      t.moves <- ClusterMap.remove cluster t.moves
    | exception Not_found ->
      ()

let complete_move t move =
  if not(ClusterMap.mem move.Move.src t.moves)
  then Log.warn (fun f -> f "Not completing move state of cluster %Ld: operation cancelled" move.Move.src)
  else t.moves <- ClusterMap.remove move.Move.src t.moves

let get_last_block t =
  let max_ref =
    try
      fst @@ ClusterMap.max_binding t.refs
    with Not_found ->
      Int64.pred t.first_movable_cluster in
  let max_root =
    try
      Int64.IntervalSet.Interval.y @@ Int64.IntervalSet.max_elt t.roots
    with Not_found ->
      max_ref in
  max max_ref max_root

let to_summary_string t =
  Printf.sprintf "total_free = %Ld; total_used = %Ld; total_roots = %Ld; max_cluster = %Ld"
    (total_free t) (total_used t) (total_roots t) (get_last_block t)

let add t rf cluster =
  let c, w = rf in
  if cluster = 0L then () else begin
    if ClusterMap.mem cluster t.refs then begin
      let c', w' = ClusterMap.find cluster t.refs in
      Log.err (fun f -> f "Found two references to cluster %Ld: %Ld.%d and %Ld.%d" cluster c w c' w');
      failwith (Printf.sprintf "Found two references to cluster %Ld: %Ld.%d and %Ld.%d" cluster c w c' w');
    end;
    t.junk <- Int64.IntervalSet.(remove (Interval.make cluster cluster) t.junk);
    t.refs <- ClusterMap.add cluster rf t.refs;
    ()
  end

let remove t cluster =
  t.junk <- Int64.IntervalSet.(add (Interval.make cluster cluster) t.junk);
  t.refs <- ClusterMap.remove cluster t.refs

(* Fold over all free blocks *)
let fold_over_free_s f t acc =
  let range i acc =
    let from = Int64.IntervalSet.Interval.x i in
    let upto = Int64.IntervalSet.Interval.y i in
    let rec loop acc x =
      let open Lwt.Infix in
      if x = (Int64.succ upto) then Lwt.return acc else begin
        f x acc >>= fun (continue, acc) ->
        if continue
        then loop acc (Int64.succ x)
        else Lwt.return acc
      end in
    loop acc from in
  Int64.IntervalSet.fold_s range t.junk acc

let with_roots t clusters f =
  t.roots <- Int64.IntervalSet.union clusters t.roots;
  Lwt.finalize f (fun () ->
    t.roots <- Int64.IntervalSet.diff t.roots clusters;
    Lwt.return_unit
  )

open Result

let compact_s f t acc =
  (* The last allocated block. Note if there are no data blocks this will
     point to the last header block even though it is immovable. *)
  let max_cluster = get_last_block t in
  let open Lwt.Infix in
  let refs = ref t.refs in
  fold_over_free_s
    (fun cluster acc -> match acc with
      | Error e -> Lwt.return (false, Error e)
      | Ok (acc, max_cluster) ->
      (* A free block after the last allocated block will not be filled.
         It will be erased from existence when the file is truncated at the
         end. *)
      if cluster >= max_cluster then Lwt.return (false, Ok (acc, max_cluster)) else begin
        (* find the last physical block *)
        let last_block, rf = ClusterMap.max_binding (!refs) in

        if cluster >= last_block then Lwt.return (false, Ok (acc, last_block)) else begin
          (* copy last_block into cluster and update rf *)
          let move = { Move.src = last_block; dst = cluster } in
          refs := ClusterMap.remove last_block @@ ClusterMap.add cluster rf (!refs);
          f move acc
          >>= function
          | Ok (continue, acc) -> Lwt.return (continue, Ok (acc, last_block))
          | Error e -> Lwt.return (false, Error e)
        end
      end
    ) t (Ok (acc, max_cluster))
  >>= function
  | Ok (result, _) -> Lwt.return (Ok result)
  | Error e -> Lwt.return (Error e)
