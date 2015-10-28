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
end

type offset = int64 with sexp

type t = {
  version: Version.t;
  backing_file_offset: offset;
  backing_file_size: int32;
  cluster_bits: int32;
  size: int64;
  crypt_method: CryptMethod.t;
  ll_size: int32;
  ll_table_offset: offset;
  refcount_table_offset: offset;
  refcount_table_clusters: int32;
  nb_snapshots: int32;
  snapshots_offset: offset;
} with sexp

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
  Int32.write t.ll_size rest
  >>= fun rest ->
  Int64.write t.ll_table_offset rest
  >>= fun rest ->
  Int64.write t.refcount_table_offset rest
  >>= fun rest ->
  Int32.write t.refcount_table_clusters rest
  >>= fun rest ->
  Int32.write t.nb_snapshots rest
  >>= fun rest ->
  Int64.write t.snapshots_offset rest

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
  >>= fun (ll_size, rest) ->
  Int64.read rest
  >>= fun (ll_table_offset, rest) ->
  Int64.read rest
  >>= fun (refcount_table_offset, rest) ->
  Int32.read rest
  >>= fun (refcount_table_clusters, rest) ->
  Int32.read rest
  >>= fun (nb_snapshots, rest) ->
  Int64.read rest
  >>= fun (snapshots_offset, rest) ->
  return ({ version; backing_file_offset; backing_file_size; cluster_bits;
    size; crypt_method; ll_size; ll_table_offset; refcount_table_offset;
    refcount_table_clusters; nb_snapshots; snapshots_offset }, rest)
