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
open Sexplib.Std

let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info);
  src

module Log = (val Logs.src_log src : Logs.LOG)

module ClusterSet = Qcow_diet.Make(struct
  type t = int64 [@@deriving sexp]
  let succ = Int64.succ
  let pred = Int64.pred
  let compare = Int64.compare
end)
module ClusterMap = Map.Make(Int64)

type cluster = int64
type reference = cluster * int

type t = {
  (* unused clusters in the file. These can be safely overwritten with new data *)
  free: ClusterSet.t;
  (* map from physical cluster to the physical cluster + offset of the reference.
     When a block is moved, this reference must be updated. *)
  refs: reference ClusterMap.t;
  first_movable_cluster: int64;
}

let make ~free ~first_movable_cluster =
  let refs = ClusterMap.empty in
  { free; refs; first_movable_cluster }

let get_free t = t.free
let get_references t = t.refs
let get_first_movable_cluster t = t.first_movable_cluster

(* mark a virtual -> physical mapping as in use *)
let mark max_cluster t rf cluster =
  let c, w = rf in
  if cluster = 0L then t else begin
    if ClusterMap.mem cluster t.refs then begin
      let c', w' = ClusterMap.find cluster t.refs in
      Log.err (fun f -> f "Found two references to cluster %Ld: %Ld.%d and %Ld.%d" cluster c w c' w');
      failwith (Printf.sprintf "Found two references to cluster %Ld: %Ld.%d and %Ld.%d" cluster c w c' w');
    end;
    if cluster > max_cluster then begin
      Log.err (fun f -> f "Found a reference to cluster %Ld outside the file (max cluster %Ld) from cluster %Ld.%d" cluster max_cluster c w);
      failwith (Printf.sprintf "Found a reference to cluster %Ld outside the file (max cluster %Ld) from cluster %Ld.%d" cluster max_cluster c w);
    end;
    let free = ClusterSet.(remove (Interval.make cluster cluster) t.free) in
    let refs = ClusterMap.add cluster rf t.refs in
    { t with free; refs }
  end

(* Fold over all free blocks *)
let fold_over_free f t acc =
  let range i acc =
    let from = ClusterSet.Interval.x i in
    let upto = ClusterSet.Interval.y i in
    let rec loop acc x =
      if x = (Int64.succ upto) then acc else loop (f x acc) (Int64.succ x) in
    loop acc from in
  ClusterSet.fold range t.free acc
