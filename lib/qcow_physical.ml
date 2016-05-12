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
open Result

let ( <| ) = Int64.shift_left
let ( |> ) = Int64.shift_right_logical

(* XXX: change to a record rather than a variant: maintain precision by
   remembering the byte value and force implementations to round on demand *)

type t = {
  bytes: int64;
  is_mutable: bool;
  is_compressed: bool;
} [@@deriving sexp]

let to_string t = Sexplib.Sexp.to_string (sexp_of_t t)

let sizeof _ = 8

let shift t bytes = { t with bytes = Int64.add t.bytes bytes }

let make ?(is_mutable = true) ?(is_compressed = false) x =
  let bytes = (x <| 2) |> 2 in
  { bytes; is_mutable; is_compressed}

let is_mutable t = t.is_mutable
let is_compressed t = t.is_compressed

(* Take an offset and round it down to the nearest physical sector, returning
   the sector number and an offset within the sector *)
let rec to_sector ~sector_size { bytes = x } =
  Int64.(div x (of_int sector_size)),
  Int64.(to_int (rem x (of_int sector_size)))

let to_bytes { bytes = x } = x

let rec to_cluster ~cluster_bits { bytes = x } =
  Int64.(div x (1L <| cluster_bits)),
  Int64.(to_int (rem x (1L <| cluster_bits)))

let read rest =
  let x = Cstruct.BE.get_uint64 rest 0 in
  let is_mutable = x |> 63 = 1L in
  let is_compressed = (x <| 1) |> 63 = 1L in
  let bytes = (x <| 2) |> 2 in
  Ok({bytes; is_mutable; is_compressed}, Cstruct.shift rest 8)

let write t rest =
  let is_mutable = if t.is_mutable then 1L <| 63 else 0L in
  let is_compressed = if t.is_compressed then 1L <| 62 else 0L in
  let raw = Int64.(logor (logor t.bytes is_mutable) is_compressed) in
  Cstruct.BE.set_uint64 rest 0 raw;
  Ok(Cstruct.shift rest 8)
