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

type offset =
  | Bytes of int64            (** octets from beginning of file *)
  | PhysicalSectors of int64  (** physical sectors on the underlying disk *)
  | Clusters of int64         (** virtual clusters in the qcow image *)
with sexp

type t = {
	offset: offset;
	copied: bool; (* refcount = 1 implies no snapshots implies direct write ok *)
	compressed: bool;
}
with sexp

let to_string t = Sexplib.Sexp.to_string (sexp_of_t t)

let sizeof _ = 8

let shift t bytes = match t.offset with
  | Bytes x -> { t with offset = Bytes (Int64.add x bytes) }
  | _ -> failwith "cannot shift non-byte offsets"

let make x =
	let offset = Bytes ((x <| 2) |> 2) in
	{ offset; copied = false; compressed = false}

(* Take an offset and round it down to the nearest physical sector, returning
   the sector number and an offset within the sector *)
let rec to_sector ~sector_size ~cluster_bits t = match t.offset with
  | Bytes x ->
    Int64.(div x (of_int sector_size)),
    Int64.(to_int (rem x (of_int sector_size)))
  | PhysicalSectors x -> x, 0
  | Clusters x ->
    let bytes = x <| cluster_bits in
    to_sector ~sector_size ~cluster_bits { t with offset = Bytes bytes }

let to_bytes ~sector_size ~cluster_bits t = match t.offset with
  | Bytes x -> x
  | PhysicalSectors x -> Int64.(mul (of_int sector_size) x)
  | Clusters x -> x <| cluster_bits

let rec to_cluster ~sector_size ~cluster_bits t = match t.offset with
  | Bytes x ->
    Int64.(div x (1L <| cluster_bits)),
    Int64.(to_int (rem x (1L <| cluster_bits)))
  | PhysicalSectors x ->
    to_cluster ~sector_size ~cluster_bits { t with offset = Bytes (Int64.(mul x (of_int sector_size))) }
  | Clusters x -> x, 0

let read rest =
  let x = Cstruct.BE.get_uint64 rest 0 in
  let copied = x |> 63 = 1L in
  let compressed = (x <| 1) |> 63 = 1L in
  let offset = Bytes ((x <| 2) |> 2) in
	Ok({offset; copied; compressed}, Cstruct.shift rest 8)

let write t rest = match t.offset with
  | Bytes x ->
	  let copied = if t.copied then 1L <| 63 else 0L in
		let compressed = if t.compressed then 1L <| 62 else 0L in
		let raw = Int64.(logor (logor x copied) compressed) in
		Cstruct.BE.set_uint64 rest 0 raw;
		Ok(Cstruct.shift rest 8)
	| _ ->
	  Error(`Msg "Offset.write needs to be converted to Bytes before marshalling")
