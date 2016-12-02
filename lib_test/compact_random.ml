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
module FromBlock = Error.FromBlock
module FromResult = Error.FromResult

open Sexplib.Std
open Qcow
open Lwt
open OUnit
open Utils
open Sizes

module Block = UnsafeBlock

let debug = ref false

(* Create a file which can store [nr_clusters], then randomly write and discard,
   checking with read whether the expected data is in each cluster. By convention
   we write the cluster index into each cluster so we can detect if they
   permute or alias. *)
let random_write_discard_compact nr_clusters =
  (* create a large disk *)
  let open Lwt.Infix in
  let module B = Qcow.Make(Block) in
  let cluster_bits = 16 in (* FIXME: avoid hardcoding this *)
  let cluster_size = 1 lsl cluster_bits in
  let size = Int64.(mul nr_clusters (of_int cluster_size)) in
  let path = Filename.concat test_dir (Int64.to_string size) ^ ".compact" in
  let t =
    truncate path
    >>= fun () ->
    let open FromBlock in
    Block.connect path
    >>= fun block ->
    B.create block ~size ()
    >>= fun qcow ->
    let h = B.header qcow in
    let open Lwt.Infix in
    B.get_info qcow
    >>= fun info ->
    let sectors_per_cluster = cluster_size / info.B.sector_size in
    let nr_sectors = Int64.(div size (of_int info.B.sector_size)) in

    (* add to this set on write, remove on discard *)
    let module SectorSet = Qcow_diet.Make(Qcow_types.Int64) in
    let written = ref SectorSet.empty in
    let empty = ref SectorSet.(add (0L, info.B.size_sectors) empty) in
    let nr_iterations = ref 0 in

    let make_cluster idx =
      let cluster = malloc cluster_size in
      for i = 0 to cluster_size / 8 - 1 do
        Cstruct.BE.set_uint64 cluster (i * 8) idx
      done;
      cluster in
    let write_cluster idx =
      let cluster = make_cluster idx in
      let n = Int64.of_int (Cstruct.len cluster / info.B.sector_size) in
      let x = Int64.(mul idx (of_int sectors_per_cluster)) in
      let y = Int64.(add x (pred n)) in
      B.write qcow x [ cluster ]
      >>= function
      | `Error _ -> failwith "write"
      | `Ok () ->
        written := SectorSet.add (x, y) !written;
        empty := SectorSet.remove (x, y) !empty;
        Lwt.return_unit in
    let discard x n =
      let y = Int64.(add x (pred n)) in
      B.discard qcow ~sector:x ~n ()
      >>= function
      | `Error _ -> failwith "discard"
      | `Ok () ->
        written := SectorSet.remove (x, y) !written;
        empty := SectorSet.add (x, y) !empty;
        Lwt.return_unit in
    let read_cluster idx =
      let cluster = malloc cluster_size in
      let x = Int64.(mul idx (of_int sectors_per_cluster)) in
      B.read qcow x [ cluster ]
      >>= function
      | `Error _ -> failwith "read"
      | `Ok () ->
        Lwt.return cluster in
    let check_contents buf expected =
      for i = 0 to (Cstruct.len buf) / 8 - 1 do
        let actual = Cstruct.BE.get_uint64 buf (i * 8) in
        if actual <> expected
        then failwith (Printf.sprintf "contents of cluster incorrect: expected %Ld but actual %Ld" expected actual)
      done in
    let check_all_clusters () =
      let rec check p set = match SectorSet.choose set with
        | x, y ->
          begin
            let n = Int64.(succ (sub y x)) in
            let buf = malloc ((Int64.to_int n) * info.B.sector_size) in
            B.read qcow x [ buf ]
            >>= function
            | `Error _ -> failwith "read"
            | `Ok () ->
              let rec for_each_sector x remaining =
                if Cstruct.len remaining = 0 then () else begin
                  let cluster = Int64.(div x (of_int sectors_per_cluster)) in
                  let expected = p cluster in
                  let sector = Cstruct.sub remaining 0 512 in
                  check_contents sector expected;
                  for_each_sector (Int64.succ x) (Cstruct.shift remaining 512)
                end in
              for_each_sector x buf;
              check p (SectorSet.remove (x, y) set)
          end
        | exception Not_found ->
          Lwt.return_unit in
      check (fun _ -> 0L) !empty;
      check (fun x -> x) !written in
    Random.init 0;
    let rec loop () =
      incr nr_iterations;
      let r = Random.int 21 in
      (* A random action: mostly a write or a discard, occasionally a compact *)
      ( if 0 <= r && r < 10 then begin
          let idx = Random.int64 nr_clusters in
          if !debug then Printf.fprintf stderr "write %Ld\n%!" idx;
          Printf.printf ".%!";
          write_cluster idx
        end else if 10 <= r && r < 20 then begin
          let sector = Random.int64 nr_sectors in
          let n = Random.int64 (Int64.sub nr_sectors sector) in
          if !debug then Printf.fprintf stderr "discard %Ld %Ld\n%!" sector n;
          Printf.printf "-%!";
          discard sector n
        end else begin
          if !debug then Printf.fprintf stderr "compact\n%!";
          Printf.printf "x%!";
          B.compact qcow ()
          >>= function
          | `Error _ -> failwith "compact"
          | `Ok _report -> Lwt.return_unit
        end )
      >>= fun () ->
      check_all_clusters ()
      >>= fun () ->
      loop () in
    Lwt.catch loop
      (fun e ->
        Printf.fprintf stderr "Test failed on iteration # %d\n%!" !nr_iterations;
        Printexc.print_backtrace stderr;
        Lwt.fail e
      ) in
  or_failwith @@ Lwt_main.run t

let _ =
  let clusters = ref 128 in
  Arg.parse [
    "-clusters", Arg.Set_int clusters, Printf.sprintf "Total number of clusters (default %d)" !clusters;
    "-debug", Arg.Set debug, "enable debug"
  ] (fun x ->
      Printf.fprintf stderr "Unexpected argument: %x\n";
      exit 1
    ) "Perform random read/write/discard/compact operations on a qcow file";

  random_write_discard_compact (Int64.of_int !clusters);
  ignore(run "rm" [ "-rf"; test_dir ])
