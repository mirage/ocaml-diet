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

module Version : sig
  type t = [
    | `One
    | `Two
    | `Three
  ] with sexp

  include S.SERIALISABLE with type t := t
end

module CryptMethod : sig
  type t = [ `Aes | `None ] with sexp

  include S.SERIALISABLE with type t := t
end

type offset = int64
(** Offset within the image *)

type t = {
  version: Version.t;
  backing_file_offset: offset;    (** offset of the backing file path *)
  backing_file_size: int32;       (** length of the backing file path *)
  cluster_bits: int32;            (** a cluster is 2 ** cluster_bits in size *)
  size: int64;                    (** virtual size of the image *)
  crypt_method: CryptMethod.t;
  ll_size: int32;                 (** number of 8-byte entries in the L1 table *)
  ll_table_offset: offset;        (** offset of the L1 table *)
  refcount_table_offset: offset;  (** offset of the refcount table *)
  refcount_table_clusters: int32; (** size of the refcount table in clusters *)
  nb_snapshots: int32;            (** the number of internal snapshots *)
  snapshots_offset: offset;       (** offset of the snapshot header *)
} with sexp
(** The qcow2 header *)

include S.SERIALISABLE with type t := t
