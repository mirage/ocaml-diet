(*
 * Copyright (C) 2015 David Scott <dave.scott@unikernel.com>
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

let ( <| ) = Int64.shift_left
let ( |> ) = Int64.shift_right_logical

type t = int64 (* the encoded form on the disk *)

let make ?(is_mutable = true) ?(is_compressed = false) x =
  let bytes = (x <| 2) |> 2 in
  let is_mutable = if is_mutable then 1L <| 63 else 0L in
  let is_compressed = if is_compressed then 1L <| 62 else 0L in
  Int64.(logor (logor bytes is_mutable) is_compressed)

let is_mutable t = t |> 63 = 1L

let is_compressed t = (t <| 1) |> 63 = 1L

let shift t bytes =
  let bytes' = (t <| 2) |> 2 in
  let is_mutable = is_mutable t in
  let is_compressed = is_compressed t in
  make ~is_mutable ~is_compressed (Int64.add bytes' bytes)

(* Take an offset and round it down to the nearest physical sector, returning
   the sector number and an offset within the sector *)
let to_sector ~sector_size t =
  let x = (t <| 2) |> 2 in
  Int64.(div x (of_int sector_size)),
  Int64.(to_int (rem x (of_int sector_size)))

let to_bytes t = (t <| 2) |> 2

let to_cluster ~cluster_bits t =
  let x = (t <| 2) |> 2 in
  Int64.(div x (1L <| cluster_bits)),
  Int64.(to_int (rem x (1L <| cluster_bits)))

let read rest =
  Cstruct.BE.get_uint64 rest 0

let write t rest =
  Cstruct.BE.set_uint64 rest 0 t

type _t = {
  bytes: int64;
  is_mutable: bool;
  is_compressed: bool;
} [@@deriving sexp]

let sexp_of_t t =
  let bytes = (t <| 2) |> 2 in
  let is_mutable = is_mutable t in
  let is_compressed = is_compressed t in
  let _t = { bytes; is_mutable; is_compressed } in
  sexp_of__t _t

let t_of_sexp s =
  let _t = _t_of_sexp s in
  let is_mutable = if _t.is_mutable then 1L <| 63 else 0L in
  let is_compressed = if _t.is_compressed then 1L <| 62 else 0L in
  Int64.(logor (logor _t.bytes is_mutable) is_compressed)

let to_string t = Sexplib.Sexp.to_string (sexp_of_t t)
