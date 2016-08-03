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
open Qcow
open Error

let expect_ok = function
  | Ok x -> x
  | Error (`Msg m) -> failwith m

let (>>*=) m f =
  let open Lwt in
  m >>= function
  | `Error x -> Lwt.return (`Error x)
  | `Ok x -> f x

let to_cmdliner_error = function
  | `Error `Disconnected -> `Error(false, "Disconnected")
  | `Error `Is_read_only -> `Error(false, "Is_read_only")
  | `Error `Unimplemented -> `Error(false, "Unimplemented")
  | `Error (`Unknown x) -> `Error(false, x)
  | `Ok x -> `Ok x

let info filename =
  let t =
    let open Lwt in
    Lwt_unix.openfile filename [ Lwt_unix.O_RDONLY ] 0
    >>= fun fd ->
    let buffer = Cstruct.create 1024 in
    Lwt_cstruct.complete (Lwt_cstruct.read fd) buffer
    >>= fun () ->
    let h, _ = expect_ok (Header.read buffer) in
    Printf.printf "%s\n" (Sexplib.Sexp.to_string_hum (Header.sexp_of_t h));
    Printf.printf "Max clusters: %Ld\n" (Int64.shift_right h.Header.size (Int32.to_int h.Header.cluster_bits));

    Printf.printf "Refcounts per cluster: %Ld\n" (Header.refcounts_per_cluster h);
    Printf.printf "Max refcount table size: %Ld\n" (Header.max_refcount_table_size h);

    return (`Ok ()) in
  Lwt_main.run t

let write filename sector data =
  let module B = Qcow.Make(Block) in
  let t =
    let open Lwt in
    Block.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok x ->
      B.connect x
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to read qcow formatted data on %s" filename)
      | `Ok x ->
        let npages = (String.length data + 4095) / 4096 in
        let buf = Io_page.(to_cstruct (get npages)) in
        Cstruct.memset buf 0;
        Cstruct.blit_from_string data 0 buf 0 (String.length data);
        B.write x sector [ buf ]
        >>= function
        | `Error _ -> failwith "write failed"
        | `Ok () -> return (`Ok ()) in
  Lwt_main.run t

let read filename sector length =
  let module B = Qcow.Make(Block) in
  let t =
    let open Lwt in
    Block.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok x ->
      B.connect x
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to read qcow formatted data on %s" filename)
      | `Ok x ->
        let length = Int64.to_int length * 512 in
        let npages = (length + 4095) / 4096 in
        let buf = Io_page.(to_cstruct (get npages)) in
        B.read x sector [ buf ]
        >>= function
        | `Error _ -> failwith "write failed"
        | `Ok () ->
          let result = Cstruct.sub buf 0 length in
          Printf.printf "%s%!" (Cstruct.to_string result);
          return (`Ok ()) in
  Lwt_main.run t

let check filename =
  let module B = Qcow.Make(Block) in
  let open Lwt in
  let t =
    Block.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok x ->
      B.connect x
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to read qcow formatted data on %s" filename)
      | `Ok x ->
        Mirage_block.fold_s ~f:(fun acc ofs buf ->
          return ()
        ) () (module B) x
        >>= function
        | `Error (`Msg m) -> failwith m
        | `Ok () ->
          return (`Ok ()) in
  Lwt_main.run t

let repair filename =
  let module B = Qcow.Make(Block) in
  let open Lwt in
  let t =
    Block.connect filename
    >>*= fun x ->
    B.connect x
    >>*= fun x ->
    B.rebuild_refcount_table x
    >>*= fun () ->
    B.Debug.check_no_overlaps x
    >>*= fun () ->
    return (`Ok ()) in
  Lwt_main.run (t >>= fun r -> return (to_cmdliner_error r))

let decode filename output =
  let module B = Qcow.Make(Block) in
  let open Lwt in
  let t =
    Block.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok x ->
      B.connect x
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to read qcow formatted data on %s" filename)
      | `Ok x ->
        B.get_info x
        >>= fun info ->
        let total_size = Int64.(mul info.B.size_sectors (of_int info.B.sector_size)) in
        Lwt_unix.openfile output [ Lwt_unix.O_WRONLY; Lwt_unix.O_CREAT ] 0o0644
        >>= fun fd ->
        Lwt_unix.LargeFile.ftruncate fd total_size
        >>= fun () ->
        Lwt_unix.close fd
        >>= fun () ->
        Block.connect output
        >>= function
        | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
        | `Ok y ->
          Mirage_block.sparse_copy (module B) x (module Block) y
          >>= function
          | `Error _ -> failwith "copy failed"
          | `Ok () -> return (`Ok ()) in
  Lwt_main.run t

let encode filename output =
  let module B = Qcow.Make(Block) in
  let open Lwt in
  let t =
    Block.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok raw_input ->
      Block.get_info raw_input
      >>= fun raw_input_info ->
      let total_size = Int64.(mul raw_input_info.Block.size_sectors (of_int raw_input_info.Block.sector_size)) in
      Lwt_unix.openfile output [ Lwt_unix.O_WRONLY; Lwt_unix.O_CREAT ] 0o0644
      >>= fun fd ->
      Lwt_unix.close fd
      >>= fun () ->
      Block.connect output
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to open %s" output)
      | `Ok raw_output ->
        B.create raw_output total_size
        >>= function
        | `Error _ -> failwith (Printf.sprintf "Failed to create qcow formatted data on %s" output)
        | `Ok qcow_output ->

          Mirage_block.sparse_copy (module Block) raw_input (module B) qcow_output
          >>= function
          | `Error (`Msg m) -> failwith m
          | `Error _ -> failwith "copy failed"
          | `Ok () -> return (`Ok ()) in
  Lwt_main.run t

let create size filename =
  let module B = Qcow.Make(Block) in
  let open Lwt in
  let t =
    Lwt_unix.openfile filename [ Lwt_unix.O_CREAT ] 0o0644
    >>= fun fd ->
    Lwt_unix.close fd
    >>= fun () ->
    Block.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok x ->
      B.create x size
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to create qcow formatted data on %s" filename)
      | `Ok x -> return (`Ok ()) in
  Lwt_main.run t
