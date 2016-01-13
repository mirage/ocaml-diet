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
open Qcow_error
module OldInt64 = Int64
open Qcow_types

let ( <| ) = OldInt64.shift_left
let ( |> ) = OldInt64.shift_right_logical

module Version = struct
  type t = [
    | `One
    | `Two
    | `Three
  ] with sexp

  let sizeof t = 4

  let write t rest =
    Int32.write (match t with | `One -> 1l | `Two -> 2l | `Three -> 3l) rest

  let read rest =
    Int32.read rest
    >>= fun (version, rest) ->
    match version with
    | 1l -> return (`One, rest)
    | 2l -> return (`Two, rest)
    | 3l -> return (`Three, rest)
    | _ -> error_msg "Unknown version: %ld" version

  let compare (a: t) (b: t) = compare a b
end

module CryptMethod = struct

  type t = [ `Aes | `None ] with sexp

  let sizeof _ = 4

  let write t rest =
    Int32.write (match t with | `Aes -> 1l | `None -> 0l) rest

  let read rest =
    Int32.read rest
    >>= fun (m, rest) ->
    match m with
    | 0l -> return (`None, rest)
    | 1l -> return (`Aes, rest)
    | _ -> error_msg "Unknown crypt_method: %ld" m

  let compare (a: t) (b: t) = compare a b
end

type offset = int64 with sexp

type extension = {
  dirty: bool;
  corrupt: bool;
  compatible_features: int64;
  autoclear_features: int64;
  refcount_order: int32;
  header_length: int32;
} with sexp

type t = {
  version: Version.t;
  backing_file_offset: offset;
  backing_file_size: int32;
  cluster_bits: int32;
  size: int64;
  crypt_method: CryptMethod.t;
  l1_size: int32;
  l1_table_offset: offset;
  refcount_table_offset: offset;
  refcount_table_clusters: int32;
  nb_snapshots: int32;
  snapshots_offset: offset;
  extension: extension option;
} with sexp

let compare (a: t) (b: t) = compare a b

let to_string t = Sexplib.Sexp.to_string_hum (sexp_of_t t)

let sizeof _ = 4 + 4 + 8 + 4 + 4 + 8 + 4 + 4 + 8 + 8 + 4 + 4 + 8

let write t rest =
  big_enough_for "Header" rest (sizeof t)
  >>= fun () ->
  Int8.write (int_of_char 'Q') rest
  >>= fun rest ->
  Int8.write (int_of_char 'F') rest
  >>= fun rest ->
  Int8.write (int_of_char 'I') rest
  >>= fun rest ->
  Int8.write 0xfb rest
  >>= fun rest ->
  Version.write t.version rest
  >>= fun rest ->
  Int64.write t.backing_file_offset rest
  >>= fun rest ->
  Int32.write t.backing_file_size rest
  >>= fun rest ->
  Int32.write t.cluster_bits rest
  >>= fun rest ->
  Int64.write t.size rest
  >>= fun rest ->
  CryptMethod.write t.crypt_method rest
  >>= fun rest ->
  Int32.write t.l1_size rest
  >>= fun rest ->
  Int64.write t.l1_table_offset rest
  >>= fun rest ->
  Int64.write t.refcount_table_offset rest
  >>= fun rest ->
  Int32.write t.refcount_table_clusters rest
  >>= fun rest ->
  Int32.write t.nb_snapshots rest
  >>= fun rest ->
  Int64.write t.snapshots_offset rest
  >>= fun rest ->
  match t.extension with
  | None -> return rest
  | Some e ->
    let incompatible_features =
      let open Int64 in
      let bits = [
        (if e.dirty then 1L <| 0 else 0L);
        (if e.corrupt then 1L <| 1 else 0L);
      ] in
      List.fold_left Int64.logor 0L bits in
    Int64.write incompatible_features rest
    >>= fun rest ->
    Int64.write e.compatible_features rest
    >>= fun rest ->
    Int64.write e.autoclear_features rest
    >>= fun rest ->
    Int32.write e.refcount_order rest
    >>= fun rest ->
    Int32.write e.header_length rest

let read rest =
  big_enough_for "Header" rest (sizeof ())
  >>= fun () ->
  Int8.read rest
  >>= fun (x, rest) ->
  ( if char_of_int x = 'Q'
    then return rest
    else error_msg "Expected magic: got %02x" x )
  >>= fun rest ->
  Int8.read rest
  >>= fun (x, rest) ->
  ( if char_of_int x = 'F'
    then return rest
    else error_msg "Expected magic: got %02x" x )
  >>= fun rest ->
  Int8.read rest
  >>= fun (x, rest) ->
  ( if char_of_int x = 'I'
    then return rest
    else error_msg "Expected magic: got %02x" x )
  >>= fun rest ->
  Int8.read rest
  >>= fun (x, rest) ->
  ( if x = 0xfb
    then return rest
    else error_msg "Expected magic: got %02x" x )
  >>= fun rest ->
  Version.read rest
  >>= fun (version, rest) ->
  Int64.read rest
  >>= fun (backing_file_offset, rest) ->
  Int32.read rest
  >>= fun (backing_file_size, rest) ->
  Int32.read rest
  >>= fun (cluster_bits, rest) ->
  Int64.read rest
  >>= fun (size, rest) ->
  CryptMethod.read rest
  >>= fun (crypt_method, rest) ->
  Int32.read rest
  >>= fun (l1_size, rest) ->
  Int64.read rest
  >>= fun (l1_table_offset, rest) ->
  Int64.read rest
  >>= fun (refcount_table_offset, rest) ->
  Int32.read rest
  >>= fun (refcount_table_clusters, rest) ->
  Int32.read rest
  >>= fun (nb_snapshots, rest) ->
  Int64.read rest
  >>= fun (snapshots_offset, rest) ->
  (match version with
    | `One | `Two -> return (None, rest)
    | _ ->
      Int64.read rest
      >>= fun (incompatible_features, rest) ->
      let dirty = Int64.logand 1L (incompatible_features |> 0) = 1L in
      let corrupt = Int64.logand 1L (incompatible_features |> 1) = 1L in
      ( if incompatible_features |> 2 <> 0L
        then error_msg "unknown incompatible_features set: 0x%Lx" incompatible_features
        else return ()
      ) >>= fun () ->
      Int64.read rest
      >>= fun (compatible_features, rest) ->
      Int64.read rest
      >>= fun (autoclear_features, rest) ->
      Int32.read rest
      >>= fun (refcount_order, rest) ->
      Int32.read rest
      >>= fun (header_length, rest) ->
      return (Some { dirty; corrupt; compatible_features; autoclear_features;
                refcount_order; header_length }, rest)
  ) >>= fun (extension, rest) ->
  return ({ version; backing_file_offset; backing_file_size; cluster_bits;
            size; crypt_method; l1_size; l1_table_offset; refcount_table_offset;
            refcount_table_clusters; nb_snapshots; snapshots_offset; extension }, rest)


let refcounts_per_cluster t =
  let cluster_bits = Int32.to_int t.cluster_bits in
  let size = t.size in
  let cluster_size = 1L <| cluster_bits in
  (* Each reference count is 2 bytes long *)
  OldInt64.div cluster_size 2L

let max_refcount_table_size t =
  let cluster_bits = Int32.to_int t.cluster_bits in
  let size = t.size in
  let cluster_size = 1L <| cluster_bits in
  let refs_per_cluster = refcounts_per_cluster t in
  let size_in_clusters = OldInt64.div (Int64.round_up size cluster_size) cluster_size in
  let refs_clusters_required = OldInt64.div (Int64.round_up size_in_clusters refs_per_cluster) refs_per_cluster in
  (* Each cluster containing references consumes 8 bytes in the
     refcount_table. How much space is that? *)
  let refcount_table_bytes = OldInt64.mul refs_clusters_required 8L in
  OldInt64.div (Int64.round_up refcount_table_bytes cluster_size) cluster_size

let l2_tables_required ~cluster_bits size =
  (* The L2 table is of size (1L <| cluster_bits) bytes
     and contains (1L <| (cluster_bits - 3)) 8-byte pointers.
     A single L2 table therefore manages
     (1L <| (cluster_bits - 3)) * (1L <| cluster_bits) bytes
     = (1L <| (2 * cluster_bits - 3)) bytes. *)
  let bytes_per_l2 = 1L <| (2 * cluster_bits - 3) in
  Int64.div (Int64.round_up size bytes_per_l2) bytes_per_l2
