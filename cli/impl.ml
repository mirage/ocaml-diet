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
    return (`Ok ()) in
  Lwt_main.run t

let check filename =
  let module B = Qcow.Client.Make(Block) in
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

let copy filename output =
  let module B = Qcow.Client.Make(Block) in
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
          Mirage_block.copy (module B) x (module Block) y
          >>= function
          | `Error _ -> failwith "copy failed"
          | `Ok () -> return (`Ok ()) in
  Lwt_main.run t

let create size filename =
  let module B = Qcow.Client.Make(Block) in
  let open Lwt in
  let t =
    Block.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok x ->
      B.create x size
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to create qcow formatted data on %s" filename)
      | `Ok x -> return (`Ok ()) in
  Lwt_main.run t
