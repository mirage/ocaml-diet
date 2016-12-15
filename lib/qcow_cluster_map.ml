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

module ClusterSet = Qcow_bitmap
module ClusterMap = Map.Make(Int64)

type cluster = int64
type reference = cluster * int

type t = {
  (* unused clusters in the file. These can be safely overwritten with new data *)
  free: ClusterSet.t;
  (* map from physical cluster to the physical cluster + offset of the reference.
     When a block is moved, this reference must be updated. *)
  mutable refs: reference ClusterMap.t;
  first_movable_cluster: int64;
}

let make ~free ~first_movable_cluster =
  let refs = ClusterMap.empty in
  { free; refs; first_movable_cluster }

let copy t =
  let free = ClusterSet.copy t.free in
  let refs = t.refs in
  let first_movable_cluster = t.first_movable_cluster in
  { free; refs; first_movable_cluster }

let total_used t =
  Int64.of_int @@ ClusterMap.cardinal t.refs

let total_free t =
  ClusterSet.fold
    (fun i acc ->
      let from = ClusterSet.Interval.x i in
      let upto = ClusterSet.Interval.y i in
      let size = Int64.succ (Int64.sub upto from) in
      Int64.add size acc
    ) t.free 0L

let add t rf cluster =
  let c, w = rf in
  if cluster = 0L then () else begin
    if ClusterMap.mem cluster t.refs then begin
      let c', w' = ClusterMap.find cluster t.refs in
      Log.err (fun f -> f "Found two references to cluster %Ld: %Ld.%d and %Ld.%d" cluster c w c' w');
      failwith (Printf.sprintf "Found two references to cluster %Ld: %Ld.%d and %Ld.%d" cluster c w c' w');
    end;
    ClusterSet.(remove (Interval.make cluster cluster) t.free);
    t.refs <- ClusterMap.add cluster rf t.refs;
    ()
  end

(* Fold over all free blocks *)
let fold_over_free_s f t acc =
  let range i acc =
    let from = ClusterSet.Interval.x i in
    let upto = ClusterSet.Interval.y i in
    let rec loop acc x =
      let open Lwt.Infix in
      if x = (Int64.succ upto) then Lwt.return acc else begin
        f x acc >>= fun (continue, acc) ->
        if continue
        then loop acc (Int64.succ x)
        else Lwt.return acc
      end in
    loop acc from in
  ClusterSet.fold_s range t.free acc

module Move = struct
  type t = { src: cluster; dst: cluster; update: reference }
end

let get_last_block t =
  try
    fst @@ ClusterMap.max_binding t.refs
  with Not_found ->
    Int64.pred t.first_movable_cluster

let compact_s f t acc =
  let t = copy t in
  (* The last allocated block. Note if there are no data blocks this will
     point to the last header block even though it is immovable. *)
  let max_cluster = get_last_block t in
  let open Lwt.Infix in

  fold_over_free_s
    (fun cluster acc -> match acc with
      | `Error e -> Lwt.return (false, `Error e)
      | `Ok (acc, max_cluster) ->
      (* A free block after the last allocated block will not be filled.
         It will be erased from existence when the file is truncated at the
         end. *)
      if cluster >= max_cluster then Lwt.return (false, `Ok (acc, max_cluster)) else begin
        (* find the last physical block *)
        let last_block, rf = ClusterMap.max_binding t.refs in

        if cluster >= last_block then Lwt.return (false, `Ok (acc, last_block)) else begin
          (* copy last_block into cluster and update rf *)
          let move = { Move.src = last_block; dst = cluster; update = rf } in
          let src_interval = ClusterSet.Interval.make last_block last_block in
          let dst_interval = ClusterSet.Interval.make cluster cluster in
          ClusterSet.add dst_interval t.free;
          ClusterSet.remove src_interval t.free;
          t.refs <- ClusterMap.remove last_block @@ ClusterMap.add cluster rf t.refs;
          f move t acc
          >>= function
          | `Ok (continue, acc) -> Lwt.return (continue, `Ok (acc, last_block))
          | `Error e -> Lwt.return (false, `Error e)
        end
      end
    ) t (`Ok (acc, max_cluster))
  >>= function
  | `Ok (result, _) -> Lwt.return (`Ok result)
  | `Error e -> Lwt.return (`Error e)
