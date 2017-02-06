(*
 * Copyright (C) 2017 Docker Inc
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

(** An in-memory cache of metadata clusters used to speed up lookups.

    Cache entries may be `read` or `update`d with a lock held to block
    concurrent access.
*)

let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info);
  src

module Log = (val Logs.src_log src : Logs.LOG)


module Lwt_error = Qcow_error.Lwt_error
module Lwt_write_error = Qcow_error.Lwt_write_error
module Cache = Qcow_cache

type error = [ Mirage_block.error | `Msg of string ]
type write_error = [ Mirage_block.write_error | `Msg of string ]

type t = {
  cache: Cache.t;
  locks: Qcow_cluster.t;
  mutable cluster_map: Qcow_cluster_map.t option; (* free/ used space map *)
  cluster_bits: int;
  m: Lwt_mutex.t;
  c: unit Lwt_condition.t;
}

type cluster = {
  t: t;
  data: Cstruct.t;
  cluster: int64;
}

module Refcounts = struct
  type t = cluster
  let of_cluster x = x
  let get t n = Cstruct.BE.get_uint16 t.data (2 * n)
  let set t n v = Cstruct.BE.set_uint16 t.data (2 * n) v
end

module Physical = struct
  type t = cluster
  let of_cluster x = x
  let get t n = Qcow_physical.read (Cstruct.shift t.data (8 * n))
  let set t n v =
    begin match t.t.cluster_map with
      | Some m ->
        (* Find the block currently being referenced so it can be marked
           as free. *)
        let existing = Qcow_physical.read (Cstruct.shift t.data (8 * n)) in
        let cluster = Qcow_physical.cluster ~cluster_bits:t.t.cluster_bits existing in
        let v' = Qcow_physical.cluster ~cluster_bits:t.t.cluster_bits v in
        Log.debug (fun f -> f "Physical.set %Ld:%d -> %s%s" t.cluster n
                      (if v = Qcow_physical.unmapped then "unmapped" else Int64.to_string v')
                      (if cluster <> 0L then ", unmapping " ^ (Int64.to_string cluster) else "")
                  );
        if cluster <> 0L then begin
          let i = Qcow_clusterset.(add (Interval.make cluster cluster) empty) in
          Qcow_cluster_map.add_to_junk m i;
          Qcow_cluster_map.remove m cluster;
        end;
        Qcow_cluster_map.add m (t.cluster, n) v'
      | None -> ()
    end;
    Qcow_physical.write v (Cstruct.shift t.data (8 * n))
  let len t = Cstruct.len t.data / 8
end

let erase cluster = Cstruct.memset cluster.data 0

let make ~cache ~cluster_bits ~locks () =
  let m = Lwt_mutex.create () in
  let c = Lwt_condition.create () in
  let cluster_map = None in
  { cache; cluster_map; cluster_bits; locks; m; c }

let set_cluster_map t cluster_map = t.cluster_map <- Some cluster_map

(** Read the contents of [cluster] and apply the function [f] with the
    lock held. *)
let read t cluster f =
  let open Lwt_error.Infix in
  Qcow_cluster.with_read_lock t.locks cluster
    (fun () ->
       Cache.read t.cache cluster
       >>= fun data ->
       f { t; data; cluster }
    )

(** Read the contents of [cluster], transform it via function [f] and write
    back the results, all with the lock held. *)
let update t cluster f =
  let open Lwt_write_error.Infix in
  Qcow_cluster.with_write_lock t.locks cluster
    (fun () ->
       Cache.read t.cache cluster
       >>= fun data ->
       f { t; data; cluster }
       >>= fun () ->
       let open Lwt.Infix in
       Cache.write t.cache cluster data
       >>= function
       | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
       | Error `Disconnected -> Lwt.return (Error `Disconnected)
       | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
       | Ok () -> Lwt.return (Ok ())
    )
