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

type t = {
  mutable nr_readers: int;
  mutable writer: bool;
  m: Lwt_mutex.t;
  c: unit Lwt_condition.t;
}
let make () =
  let nr_readers = 0 in
  let writer = false in
  let m = Lwt_mutex.create () in
  let c = Lwt_condition.create () in
  { nr_readers; writer; m; c }
let with_read_lock t f =
  let open Lwt.Infix in
  Lwt_mutex.with_lock t.m
    (fun () ->
      let rec wait () =
        if t.writer then begin
          Lwt_condition.wait t.c ~mutex:t.m
          >>= fun () ->
          wait ()
        end else begin
          t.nr_readers <- t.nr_readers + 1;
          Lwt.return_unit
        end in
      wait ()
    )
  >>= fun () ->
  Lwt.finalize f
    (fun () ->
      t.nr_readers <- t.nr_readers - 1;
      Lwt_condition.signal t.c ();
      Lwt.return_unit
    )
let with_write_lock t f =
  let open Lwt.Infix in
  Lwt_mutex.with_lock t.m
    (fun () ->
      let rec wait () =
        if t.nr_readers > 0 || t.writer then begin
          Lwt_condition.wait t.c ~mutex:t.m
          >>= fun () ->
          wait ()
        end else begin
          t.writer <- true;
          Lwt.return_unit
        end in
      wait ()
    )
  >>= fun () ->
  Lwt.finalize f
    (fun () ->
      t.writer <- false;
      Lwt_condition.broadcast t.c ();
      Lwt.return_unit
    )
