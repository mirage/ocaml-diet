(*
 * Copyright (C) 2015 David Scott <dave@recoil.org>
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
open Result
open Types
open Error

module Make(B: V1_LWT.BLOCK) = struct

  type 'a io = 'a Lwt.t
  type error = B.error
  type info = {
    read_write : bool;
    sector_size : int;
    size_sectors : int64;
  }

  type id = B.id
  type page_aligned_buffer = B.page_aligned_buffer
  type t = {
    base: B.t;
    info: info
  }

  let get_info t = Lwt.return t.info

  let read t ofs bufs = B.read t.base ofs bufs
  let write t ofs bufs = B.write t.base ofs bufs

  let disconnect t = B.disconnect t.base

  let connect base =
    let open Lwt in
    B.get_info base
    >>= fun info ->
    let info' = {
      read_write = false;
      sector_size = info.B.sector_size;
      size_sectors = info.B.size_sectors
    } in
    Lwt.return (`Ok { base; info = info' })
end
