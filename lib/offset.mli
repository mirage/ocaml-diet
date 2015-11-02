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

type t = {
	bytes: int64;
	copied: bool; (* refcount = 1 implies no snapshots implies direct write ok *)
	compressed: bool;
}
with sexp

val shift: t -> int64 -> t
(** [shift t bytes] adds [bytes] to t, maintaining other properties *)

val make: ?copied:bool -> ?compressed:bool -> int64 -> t
(** Create an offset at the given byte address. This defaults to [copied = true]
    which meand there are no snapshots implying that directly writing to this
		offset is ok; and [compressed = false]. *)

val to_sector: sector_size:int -> t -> int64 * int
(** Return the sector on disk, plus a remainder within the sector *)

val to_bytes: t -> int64
(** Return the byte offset on disk *)

val to_cluster: cluster_bits:int -> t -> int64 * int
(** Return the cluster offset on disk, plus a remainder within the cluster *)

include S.PRINTABLE with type t := t
include S.SERIALISABLE with type t := t
