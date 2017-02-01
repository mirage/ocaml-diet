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
let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info);
  src

module Log = (val Logs.src_log src : Logs.LOG)

module Int64Map = Map.Make(Int64)

type cluster = int64

type t = {
  mutable locks: (Qcow_rwlock.t * int) Int64Map.t;
}

let make () =
  let locks = Int64Map.empty in
  { locks  }

let get_lock t cluster =
  let lock, refcount =
    if Int64Map.mem cluster t.locks
    then Int64Map.find cluster t.locks
    else begin
      Qcow_rwlock.make (), 0
    end in
  t.locks <- Int64Map.add cluster (lock, refcount) t.locks;
  lock

let put_lock t cluster =
  (* put_lock is always called after get_lock *)
  assert (Int64Map.mem cluster t.locks);
  let lock, refcount = Int64Map.find cluster t.locks in
  t.locks <-
    if refcount = 1
    then Int64Map.remove cluster t.locks
    else Int64Map.add cluster (lock, refcount - 1) t.locks

let with_lock t cluster f =
  let lock = get_lock t cluster in
  Lwt.finalize (fun () -> f lock) (fun () -> put_lock t cluster; Lwt.return_unit)

let with_read_lock t cluster f =
  with_lock t cluster
    (fun rw ->
      Qcow_rwlock.with_read_lock rw f
    )

let with_write_lock t cluster f =
  with_lock t cluster
    (fun rw ->
      Qcow_rwlock.with_read_lock rw f
    )
