(*
 * Copyright (C) 2017 David Scott <dave@recoil.org>
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
open Qcow_types

type t = {
  mutable locks: (Qcow_rwlock.t * int) Cluster.Map.t;
  metadata_m: Lwt_mutex.t;
  (** held during metadata changing operations *)
}

let make () =
  let locks = Cluster.Map.empty in
  let metadata_m = Lwt_mutex.create () in
  { locks; metadata_m  }

let with_metadata_lock t = Lwt_mutex.with_lock t.metadata_m

let get_lock t cluster =
  let lock, refcount =
    if Cluster.Map.mem cluster t.locks
    then Cluster.Map.find cluster t.locks
    else begin
      Qcow_rwlock.make (), 0
    end in
  t.locks <- Cluster.Map.add cluster (lock, refcount + 1) t.locks;
  lock

let put_lock t cluster =
  (* put_lock is always called after get_lock *)
  assert (Cluster.Map.mem cluster t.locks);
  let lock, refcount = Cluster.Map.find cluster t.locks in
  t.locks <-
    if refcount = 1
    then Cluster.Map.remove cluster t.locks
    else Cluster.Map.add cluster (lock, refcount - 1) t.locks

let with_lock t cluster f =
  let lock = get_lock t cluster in
  Lwt.finalize (fun () -> f lock) (fun () -> put_lock t cluster; Lwt.return_unit)

let with_read_lock t cluster f =
  with_lock t cluster
    (fun rw ->
      Qcow_rwlock.with_read_lock rw f
    )

let with_read_locks t ~first ~last f =
  let rec loop n =
    if n > last
    then f ()
    else
      with_lock t n
        (fun rw ->
          Qcow_rwlock.with_read_lock rw
            (fun () -> loop (Cluster.succ n))
        ) in
  loop first

let with_write_lock t cluster f =
  with_lock t cluster
    (fun rw ->
      Qcow_rwlock.with_read_lock rw f
    )

let with_write_locks t ~first ~last f =
  let rec loop n =
    if n > last
    then f ()
    else
      with_lock t n
        (fun rw ->
          Qcow_rwlock.with_write_lock rw
            (fun () -> loop (Cluster.succ n))
        ) in
  loop first
