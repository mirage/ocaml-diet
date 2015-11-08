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
open Sexplib.Std
open Qcow
open Lwt
open OUnit

let expect_ok = function
  | `Error _ -> failwith "IO failure"
  | `Ok x -> x

(* qemu-img will set version = `Three and leave an extra cluster
   presumably for extension headers *)

let read_write_header name size =
  let module B = Qcow.Make(Ramdisk) in
  let t =
    Ramdisk.connect name
    >>= fun x ->
    let ramdisk = expect_ok x in

    B.create ramdisk size
    >>= fun x ->
    let b = expect_ok x in

    let page = Io_page.(to_cstruct (get 1)) in
    Ramdisk.read ramdisk 0L [ page ]
    >>= fun x ->
    let () = expect_ok x in
    let open Error in
    match Qcow.Header.read page with
    | Result.Error (`Msg m) -> failwith m
    | Result.Ok (hdr, _) ->
      Lwt.return hdr in
  Lwt_main.run t

let create_1K () =
  let hdr = read_write_header "1K" 1024L in
  let expected = {
    Qcow.Header.version = `Two; backing_file_offset = 0L;
    backing_file_size = 0l; cluster_bits = 16l; size = 1024L;
    crypt_method = `None; l1_size = 1l; l1_table_offset = 131072L;
    refcount_table_offset = 65536L; refcount_table_clusters = 1l;
    nb_snapshots = 0l; snapshots_offset = 0L
  } in
  let cmp a b = Qcow.Header.compare a b = 0 in
  let printer = Qcow.Header.to_string in
  assert_equal ~printer ~cmp expected hdr

let create_1M () =
  let hdr = read_write_header "1M" 1048576L in
  let expected = {
    Qcow.Header.version = `Two; backing_file_offset = 0L;
    backing_file_size = 0l; cluster_bits = 16l; size = 1048576L;
    crypt_method = `None; l1_size = 1l; l1_table_offset = 131072L;
    refcount_table_offset = 65536L; refcount_table_clusters = 1l;
    nb_snapshots = 0l; snapshots_offset = 0L
  } in
  let cmp a b = Qcow.Header.compare a b = 0 in
  let printer = Qcow.Header.to_string in
  assert_equal ~printer ~cmp expected hdr

let mib = Int64.mul 1024L 1024L
let gib = Int64.mul mib 1024L
let tib = Int64.mul gib 1024L
let pib = Int64.mul tib 1024L

let create_1P () =
  let hdr = read_write_header "1P" pib in
  let expected = {
    Qcow.Header.version = `Two; backing_file_offset = 0L;
    backing_file_size = 0l; cluster_bits = 16l; size = pib;
    crypt_method = `None; l1_size = 2097152l; l1_table_offset = 131072L;
    refcount_table_offset = 65536L; refcount_table_clusters = 1l;
    nb_snapshots = 0l; snapshots_offset = 0L
  } in
  let cmp a b = Qcow.Header.compare a b = 0 in
  let printer = Qcow.Header.to_string in
  assert_equal ~printer ~cmp expected hdr

let boundaries cluster_bits =
  let cluster_size = Int64.shift_left 1L cluster_bits in
  let pointers_in_cluster = Int64.(div cluster_size 8L) in [
    "0", 0L;
    Printf.sprintf "one %Ld byte cluster" cluster_size, cluster_size;
    Printf.sprintf "one L2 table (containing %Ld 8-byte pointers to cluster)"
      pointers_in_cluster,
      Int64.(mul cluster_size pointers_in_cluster);
    Printf.sprintf "one L1 table (containing %Ld 8-byte pointers to L2 tables)"
      pointers_in_cluster,
      Int64.(mul (mul cluster_size pointers_in_cluster) pointers_in_cluster)
    ]

let sizes sector_size cluster_bits = [
  "one sector", Int64.of_int sector_size;
  "one page", 4096L;
  "one cluster", Int64.shift_left 1L cluster_bits;
]

let off_by ((label', offset'), (label, offset)) = [
  label, offset;
  label ^ " + " ^ label', Int64.add offset offset';
  label ^ " - " ^ label', Int64.sub offset offset';
  label ^ " + 2 * " ^ label', Int64.(add offset (mul 2L offset'));
]

let rec cross xs ys = match xs, ys with
  | [], ys -> []
  | x :: xs, ys -> List.map (fun y -> x, y) ys @ (cross xs ys)

(* Parameterise over sector, page, cluster, more *)
let interesting_ranges sector_size size_sectors cluster_bits =
  let size_bytes = Int64.(mul size_sectors (of_int sector_size)) in
  let starts = List.concat (List.map off_by (cross (sizes sector_size cluster_bits) (boundaries cluster_bits))) in
  let all = starts @ (List.map (fun (label, offset) -> label ^ " from the end", Int64.sub size_bytes offset) starts) in
  (* add lengths *)
  let all = List.map (fun ((label', length'), (label, offset)) ->
    label' ^ " @ " ^ label, offset, length'
  ) (cross (sizes sector_size cluster_bits) all) in
  List.filter
    (fun (label, offset, length) ->
      offset >= 0L && (Int64.add offset length <= size_bytes)
    ) all

let get_id =
  let next = ref 1 in
  fun () ->
    let this = !next in
    incr next;
    this

let malloc (length: int) =
  let npages = (length + 4095)/4096 in
  Cstruct.sub Io_page.(to_cstruct (get npages)) 0 length

let rec fragment into remaining =
  if into >= Cstruct.len remaining
  then [ remaining ]
  else
    let this = Cstruct.sub remaining 0 into in
    let rest = Cstruct.shift remaining into in
    this :: (fragment into rest)

module Extent = struct
  open Int64

  type t = {
    start: int64;
    length: int64;
  } with sexp
  type ts = t list with sexp

  let to_string t = Sexplib.Sexp.to_string_hum (sexp_of_ts t)

  type overlap =
    | AABB
    | BBAA
    | BABA
    | BAAB
    | ABBA
    | ABAB
  with sexp

  let classify ({ start = a_start; length = a_length } as a) ({ start = b_start; length = b_length } as b) =
    let a_end = add a_start a_length in
    let b_end = add b_start b_length in
    if b_end < a_start
    then BBAA
    else if a_end < b_start
    then AABB
    else begin
      (* there is some overlap *)
      if b_start < a_start then begin
        if b_end < a_end then BABA else BAAB
      end else begin
        if b_end < a_end then ABBA else ABAB
      end
    end

  let difference ({ start = a_start; length = a_length } as a) ({ start = b_start; length = b_length } as b) =
    let a_end = add a_start a_length in
    let b_end = add b_start b_length in
    match classify a b with
    | BBAA | AABB -> [ a ]
    | BABA -> [ { start = b_end; length = sub a_end b_end } ]
    | BAAB -> [ ]
    | ABBA -> [ { start = a_start; length = sub b_start a_start; };
                { start = b_end; length = sub a_end b_end } ]
    | ABAB -> [ { start = a_start; length = sub b_start a_start } ]

  let intersect ({ start = a_start; length = a_length } as a) ({ start = b_start; length = b_length } as b) : t list =
    let a_end = add a_start a_length in
    let b_end = add b_start b_length in
    match classify a b with
    | BBAA | AABB -> [ ]
    | BABA -> [ { start = a_start; length = sub b_end a_start } ]
    | BAAB -> [ { start = a_start; length = sub a_end a_start } ]
    | ABBA -> [ { start = b_start; length = sub b_end b_start } ]
    | ABAB -> [ { start = b_start; length = sub a_end b_start } ]
end

let read_write sector_size size_sectors (start, length) () =
  let module B = Qcow.Make(Ramdisk) in
  let t =
    Ramdisk.destroy ~name:"test";
    Ramdisk.connect "test"
    >>= fun x ->
    let ramdisk = expect_ok x in
    B.create ramdisk Int64.(mul size_sectors (of_int sector_size))
    >>= fun x ->
    let b = expect_ok x in

    let sector = Int64.div start 512L in
    let id = get_id () in
    let buf = malloc length in
    Cstruct.memset buf (id mod 256);
    B.write b sector (fragment 4096 buf)
    >>= fun x ->
    let () = expect_ok x in
    let buf' = malloc length in
    B.read b sector (fragment 4096 buf')
    >>= fun x ->
    let () = expect_ok x in
    let cmp a b = Cstruct.compare a b = 0 in
    assert_equal ~printer:(fun x -> String.escaped (Cstruct.to_string x)) ~cmp buf buf';

    (* This is the range that we expect to see written *)
    let expected = { Extent.start = sector; length = Int64.(div (of_int length) 512L) } in
    let ofs' = Int64.(mul sector (of_int sector_size)) in
    Mirage_block.fold_mapped_s
      ~f:(fun bytes_seen ofs data ->
        let actual = { Extent.start = ofs; length = Int64.of_int (Cstruct.len data / 512) } in
        (* Any data we read now which wasn't expected must be full of zeroes *)
        let extra = Extent.difference actual expected in
        List.iter
          (fun { Extent.start; length } ->
            let buf = Cstruct.sub data (512 * Int64.(to_int (sub start ofs))) (Int64.to_int length * 512) in
            for i = 0 to Cstruct.len buf - 1 do
              assert_equal ~printer:string_of_int ~cmp:(fun a b -> a = b) 0 (Cstruct.get_uint8 buf i)
            done;
          ) extra;
        let common = Extent.intersect actual expected in
        List.iter
          (fun { Extent.start; length } ->
            let buf = Cstruct.sub data (512 * Int64.(to_int (sub start ofs))) (Int64.to_int length * 512) in
            for i = 0 to Cstruct.len buf - 1 do
              assert_equal ~printer:string_of_int ~cmp:(fun a b -> a = b) (id mod 256) (Cstruct.get_uint8 buf i)
            done;
          ) common;
        let seen_this_time = 512 * List.(fold_left (+) 0 (map (fun e -> Int64.to_int e.Extent.length) common)) in
          return (`Ok (bytes_seen + seen_this_time))
        ) 0 (module B) b
    >>= fun x ->
    let total_bytes_seen = expect_ok x in
    assert_equal ~printer:string_of_int length total_bytes_seen;
    B.Debug.check_no_overlaps b
    >>= fun x ->
    let () = expect_ok x in
    Lwt.return () in
  Lwt_main.run t

let _ =
  let sector_size = 512 in
  (* Test with a 1 PiB disk, bigger than we'll need for a while. *)
  let size_sectors = Int64.div pib 512L in
  let cluster_bits = 16 in
  let interesting_writes = List.map
    (fun (label, start, length) -> label >:: read_write sector_size size_sectors (start, Int64.to_int length))
    (interesting_ranges sector_size size_sectors cluster_bits) in

  let suite = "qcow2" >::: [
    "create 1K" >:: create_1K;
    "create 1M" >:: create_1M;
    "create 1P" >:: create_1P;
  ] @ interesting_writes in
  OUnit2.run_test_tt_main (ounit2_of_ounit1 suite)
