(*
 * Copyright (C) 2013 Citrix Inc
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
 * REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
 * INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
 * LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
 * OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 *)
open Qcow
open Lwt
open OUnit

let expect_ok = function
  | `Error _ -> failwith "IO failure"
  | `Ok x -> x

(* qemu-img will set version = `Three and leave an extra cluster
   presumably for extension headers *)

let read_write_header name size =
  let module B = Qcow.Client.Make(Ramdisk) in
  let t =
    B.connect "1K"
    >>= fun x ->
    let b = expect_ok x in

    B.create b 1024L
    >>= fun x ->
    let () = expect_ok x in

    let page = Io_page.(to_cstruct (get 1)) in
    B.read b 0L [ page ]
    >>= fun x ->
    let () = expect_ok x in
    let open Error in
    Header.read page
    >>= fun (hdr, _) ->
    Lwt.return hdr in
  Lwt_main.run t

let create_1K () =
  let hdr = read_write_header "1K" 1024L in
  let expected = {
    Header.version = `Two; backing_file_offset = 0L;
    backing_file_size = 0l; cluster_bits = 16l; size = 1024L;
    crypt_method = `None; l1_size = 1l; l1_table_offset = 131072L;
    refcount_table_offset = 65536L; refcount_table_clusters = 1l;
    nb_snapshots = 0l; snapshots_offset = 0L
  } in
  let cmp a b = Header.compare a b = 0 in
  let printer = Header.to_string in
  assert_equal ~printer ~cmp expected hdr

let create_1M () =
  let hdr = read_write_header "1M" 1048576L in
  let expected = {
    Header.version = `Two; backing_file_offset = 0L;
    backing_file_size = 0l; cluster_bits = 16l; size = 1048576L;
    crypt_method = `None; l1_size = 1l; l1_table_offset = 131072L;
    refcount_table_offset = 65536L; refcount_table_clusters = 1l;
    nb_snapshots = 0l; snapshots_offset = 0L
  } in
  let cmp a b = Header.compare a b = 0 in
  let printer = Header.to_string in
  assert_equal ~printer ~cmp expected hdr

let create_1P () =
  let mib = Int64.mul 1024L 1024L in
  let gib = Int64.mul mib 1024L in
  let tib = Int64.mul gib 1024L in
  let pib = Int64.mul tib 1024L in
  let hdr = read_write_header "1P" pib in
  let expected = {
    Header.version = `Two; backing_file_offset = 0L;
    backing_file_size = 0l; cluster_bits = 16l; size = pib;
    crypt_method = `None; l1_size = 2097152l; l1_table_offset = 131072L;
    refcount_table_offset = 65536L; refcount_table_clusters = 1l;
    nb_snapshots = 0l; snapshots_offset = 0L
  } in
  let cmp a b = Header.compare a b = 0 in
  let printer = Header.to_string in
  assert_equal ~printer ~cmp expected hdr

let _ =
  let suite = "qcow2" >::: [
    "create 1K" >:: create_1K;
    "create 1M" >:: create_1M;
    "create 1P" >:: create_1P;
  ] in
  OUnit2.run_test_tt_main (ounit2_of_ounit1 suite)
