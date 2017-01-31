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
module Lwt_error = Error.Lwt_error
module Lwt_write_error = Error.Lwt_write_error
module FromResult = Error.FromResult

open Utils

module Block = UnsafeBlock

let debug = ref false

(* Create a file which can store [nr_clusters], then randomly write and discard,
   checking with read whether the expected data is in each cluster. By convention
   we write the cluster index into each cluster so we can detect if they
   permute or alias. *)
let random_write_discard_compact nr_clusters stop_after =
  (* create a large disk *)
  let open Lwt.Infix in
  let module B = Qcow.Make(Block)(Time) in
  let cluster_bits = 16 in (* FIXME: avoid hardcoding this *)
  let cluster_size = 1 lsl cluster_bits in
  let size = Int64.(mul nr_clusters (of_int cluster_size)) in
  let path = Filename.concat test_dir (Int64.to_string size) ^ ".compact" in
  let t =
    truncate path
    >>= fun () ->
    Block.connect path
    >>= fun block ->
    let config = B.Config.create ~recycle_threshold:2048L ~discard:true () in
    B.create block ~size ~lazy_refcounts:false ~config ()
    >>= function
    | Error _ -> failwith "B.create failed"
    | Ok qcow ->
    B.get_info qcow
    >>= fun info ->
    let sectors_per_cluster = cluster_size / info.Mirage_block.sector_size in
    let nr_sectors = Int64.(div size (of_int info.Mirage_block.sector_size)) in

    (* add to this set on write, remove on discard *)
    let module SectorSet = Qcow_diet.Make(Qcow_types.Int64) in
    let written = ref SectorSet.empty in
    let i = SectorSet.Interval.make 0L (Int64.pred info.Mirage_block.size_sectors) in
    let empty = ref SectorSet.(add i empty) in
    let nr_iterations = ref 0 in

    let write x n =
      assert (Int64.add x n <= nr_sectors);
      let y = Int64.(add x (pred n)) in
      let buf = malloc (info.Mirage_block.sector_size * (Int64.to_int n)) in
      let rec for_each_sector x remaining =
        if Cstruct.len remaining = 0 then () else begin
          let cluster = Int64.(div x (of_int sectors_per_cluster)) in
          let sector = Cstruct.sub remaining 0 512 in
          for i = 0 to Cstruct.len sector / 8 - 1 do
            Cstruct.BE.set_uint64 sector (i * 8) cluster
          done;
          for_each_sector (Int64.succ x) (Cstruct.shift remaining 512)
        end in
      for_each_sector x buf;
      B.write qcow x [ buf ]
      >>= function
      | Error _ -> failwith "write"
      | Ok () ->
        if n > 0L then begin
          let i = SectorSet.Interval.make x y in
          written := SectorSet.add i !written;
          empty := SectorSet.remove i !empty;
        end;
        Lwt.return_unit in
    let discard x n =
      assert (Int64.add x n <= nr_sectors);
      let y = Int64.(add x (pred n)) in
      B.discard qcow ~sector:x ~n ()
      >>= function
      | Error _ -> failwith "discard"
      | Ok () ->
      if n > 0L then begin
        let i = SectorSet.Interval.make x y in
        written := SectorSet.remove i !written;
        empty := SectorSet.add i !empty;
      end;
      Lwt.return_unit in
    let check_contents sector buf expected =
      for i = 0 to (Cstruct.len buf) / 8 - 1 do
        let actual = Cstruct.BE.get_uint64 buf (i * 8) in
        if actual <> expected
        then failwith (Printf.sprintf "contents of sector %Ld incorrect: expected %Ld but actual %Ld" sector expected actual)
      done in
    let check_all_clusters () =
      let rec check p set = match SectorSet.choose set with
        | i ->
          let x = SectorSet.Interval.x i in
          let y = SectorSet.Interval.y i in
          begin
            let n = Int64.(succ (sub y x)) in
            assert (Int64.add x n <= nr_sectors);
            let buf = malloc ((Int64.to_int n) * info.Mirage_block.sector_size) in
            B.read qcow x [ buf ]
            >>= function
            | Error _ -> failwith "read"
            | Ok () ->
              let rec for_each_sector x remaining =
                if Cstruct.len remaining = 0 then () else begin
                  let cluster = Int64.(div x (of_int sectors_per_cluster)) in
                  let expected = p cluster in
                  let sector = Cstruct.sub remaining 0 512 in
                  check_contents x sector expected;
                  for_each_sector (Int64.succ x) (Cstruct.shift remaining 512)
                end in
              for_each_sector x buf;
              check p (SectorSet.remove i set)
          end
        | exception Not_found ->
          Lwt.return_unit in
      Lwt.pick [
        check (fun _ -> 0L) !empty;
        Lwt_unix.sleep 5. >>= fun () -> Lwt.fail (Failure "check empty")
      ]
      >>= fun () ->
      Lwt.pick [
        check (fun x -> x) !written;
        Lwt_unix.sleep 5. >>= fun () -> Lwt.fail (Failure "check written")
      ] in
    Random.init 0;
    let rec loop () =
      incr nr_iterations;
      if !nr_iterations = stop_after then Lwt.return (Ok ()) else begin
        (* Erase any outstanding blocks and call flush so they become reusable *)
        B.Debug.erase_all qcow
        >>= function
        | Error _ -> failwith "erase_all"
        | Ok () ->
        B.Debug.flush qcow
        >>= function
        | Error _ -> failwith "flush"
        | Ok () ->
        let r = Random.int 21 in
        (* A random action: mostly a write or a discard, occasionally a compact *)
        ( if 0 <= r && r < 10 then begin
            let sector = Random.int64 nr_sectors in
            let n = Random.int64 (Int64.sub nr_sectors sector) in
            if !debug then Printf.fprintf stderr "write %Ld %Ld\n%!" sector n;
            Printf.printf ".%!";
            Lwt.pick [
              write sector n;
              Lwt_unix.sleep 5. >>= fun () -> Lwt.fail (Failure "write timeout")
            ]
          end else if 10 <= r && r < 20 then begin
            let sector = Random.int64 nr_sectors in
            let n = Random.int64 (Int64.sub nr_sectors sector) in
            if !debug then Printf.fprintf stderr "discard %Ld %Ld\n%!" sector n;
            Printf.printf "-%!";
            Lwt.pick [
              discard sector n;
              Lwt_unix.sleep 5. >>= fun () -> Lwt.fail (Failure "discard timeout")
            ]
          end else begin
            if !debug then Printf.fprintf stderr "compact\n%!";
            let cancel_at_percent = Random.int 300 in
            Printf.printf "x%!";
            let th = ref None in
            let progress_cb ~percent =
              if percent >= cancel_at_percent then match !th with
                | Some th' ->
                  th := None;
                  Printf.printf "X%!";
                  Lwt.cancel th'
                | None -> () in
            let t = B.compact ~progress_cb qcow () in
            th := Some t;
            Lwt.catch
              (fun () ->
                Lwt.pick [
                  t;
                  Lwt_unix.sleep 5. >>= fun () -> Lwt.fail_with "compact timeout"
                ]
                >>= function
                | Error _ -> failwith "compact"
                | Ok _report -> Lwt.return_unit
              ) (function
                | Lwt.Canceled -> Lwt.return_unit
                | e -> Lwt.fail e
              )
          end )
        >>= fun () ->
        check_all_clusters ();
        >>= fun () ->
        loop ()
      end in
    Lwt.catch loop
      (fun e ->
        Printf.fprintf stderr "Test failed on iteration # %d\n%!" !nr_iterations;
        Printexc.print_backtrace stderr;
        let s = Sexplib.Sexp.to_string_hum (SectorSet.sexp_of_t !written) in
        Lwt_io.open_file ~flags:[Unix.O_CREAT; Unix.O_TRUNC; Unix.O_WRONLY ] ~perm:0o644 ~mode:Lwt_io.output "/tmp/written.sexp"
        >>= fun oc ->
        Lwt_io.write oc s
        >>= fun () ->
        Lwt_io.close oc
        >>= fun () ->
        let s = Sexplib.Sexp.to_string_hum (SectorSet.sexp_of_t !empty) in
        Lwt_io.open_file ~flags:[Unix.O_CREAT; Unix.O_TRUNC; Unix.O_WRONLY ] ~perm:0o644 ~mode:Lwt_io.output "/tmp/empty.sexp"
        >>= fun oc ->
        Lwt_io.write oc s
        >>= fun () ->
        Lwt_io.close oc
        >>= fun () ->
        Printf.fprintf stderr ".qcow2 file is at: %s\n" path;
        Lwt.fail e
      ) in
  or_failwith @@ Lwt_main.run t

let _ =
  Logs.set_reporter (Logs_fmt.reporter ());
  let clusters = ref 128 in
  let stop_after = ref 1024 in
  Arg.parse [
    "-clusters", Arg.Set_int clusters, Printf.sprintf "Total number of clusters (default %d)" !clusters;
    "-stop-after", Arg.Set_int stop_after, Printf.sprintf "Number of iterations to stop after (default: 1024, 0 means never)";
    "-debug", Arg.Set debug, "enable debug"
  ] (fun x ->
      Printf.fprintf stderr "Unexpected argument: %s\n" x;
      exit 1
    ) "Perform random read/write/discard/compact operations on a qcow file";

  random_write_discard_compact (Int64.of_int !clusters) (!stop_after)
