(*
 * Copyright (C) 2016 Unikernel Systems
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

(* Wrappers for qemu-img, qemu-nbd to allow us to compare the contents of
   ocaml-qcow images and qemu-produced images. *)
open Utils

module Img = struct
  let create file size =
    ignore_output @@ run "qemu-img" [ "create"; "-f"; "qcow2"; "-o"; "lazy_refcounts=on"; file; Int64.to_string size ]

  let check file =
    ignore_output @@ run "qemu-img" [ "check"; file ]

  type info = {
    virtual_size: int64;
    filename: string;
    cluster_size: int;
    actual_size: int;
    compat: string;
    lazy_refcounts: bool option;
    refcount_bits: int option;
    corrupt: bool option;
    dirty_flag: bool;
  }

  let info file =
    let lines, _ = run "qemu-img" [ "info"; "--output"; "json"; file ] in
    let json = Ezjsonm.(get_dict @@ from_string @@ String.concat "\n" lines) in
    let find name json =
      if List.mem_assoc name json
      then List.assoc name json
      else failwith (Printf.sprintf "Failed to find '%s' in %s" name (String.concat "\n" lines)) in
    let virtual_size = Ezjsonm.get_int64 @@ find "virtual-size" json in
    let filename = Ezjsonm.get_string @@ find "filename" json in
    let cluster_size = Ezjsonm.get_int @@ find "cluster-size" json in
    let format = Ezjsonm.get_string @@ find "format" json in
    if format <> "qcow2" then failwith (Printf.sprintf "Expected qcow2 format, got %s" format);
    let actual_size = Ezjsonm.get_int @@ find "actual-size" json in
    let specific = Ezjsonm.get_dict @@ find "format-specific" json in
    let ty = Ezjsonm.get_string @@ find "type" specific in
    if ty <> "qcow2" then failwith (Printf.sprintf "Expected qcow2 type, got %s" ty);
    let data = Ezjsonm.get_dict @@ find "data" specific in
    let compat = Ezjsonm.get_string @@ find "compat" data in
    let lazy_refcounts = try Some (Ezjsonm.get_bool @@ find "lazy-refcounts" data) with _ -> None in
    let refcount_bits = try Some (Ezjsonm.get_int @@ find "refcount-bits" data) with _ -> None in
    let corrupt = try Some (Ezjsonm.get_bool @@ find "corrupt" data) with _ -> None in
    let dirty_flag = Ezjsonm.get_bool @@ find "dirty-flag" json in
    { virtual_size; filename; cluster_size; actual_size; compat;
      lazy_refcounts; refcount_bits; corrupt; dirty_flag }
end

module Block = struct

  type info = {
    read_write: bool;
    sector_size: int;
    size_sectors: int64;
  }

  type t = {
    server: process;
    client: Nbd_lwt_unix.Client.t;
    s: Lwt_unix.file_descr;
    info: info;
  }

  let get_info { info } = Lwt.return info

  type id = unit
  type 'a io = 'a Lwt.t
  type page_aligned_buffer = Cstruct.t
  type error = Mirage_block.Error.error

  let read { client } sector bufs =
    Nbd_lwt_unix.Client.read client (Int64.mul sector 512L) bufs

  let write { client } sector bufs =
    Nbd_lwt_unix.Client.write client (Int64.mul sector 512L) bufs

  let connect file =
    let open Lwt.Infix in
    let socket = Filename.(concat (get_temp_dir_name()) "qcow.socket") in
    (try Unix.unlink socket with Unix.Unix_error(Unix.ENOENT, _, _) -> ());
    let server = start "qemu-nbd" [ "--socket"; socket; "-f"; "qcow2"; file] in
    let rec connect () =
      let s = Lwt_unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
      Lwt.catch
        (fun () ->
          Lwt_unix.connect s (Unix.ADDR_UNIX socket)
          >>= fun () ->
          Lwt.return s)
        (fun _ ->
          Lwt_unix.sleep 0.1
          >>= fun () ->
          Lwt_unix.close s
          >>= fun () ->
          connect ()) in
    connect ()
    >>= fun s ->
    let channel = Nbd_lwt_unix.of_fd s in
    Nbd_lwt_unix.Client.negotiate channel ""
    >>= fun (client, size, flags) ->
    let read_write = not(List.mem Nbd.Protocol.PerExportFlag.Read_only flags) in
    Nbd_lwt_unix.Client.get_info client
    >>= function { Nbd_lwt_unix.Client.sector_size } ->
    assert (sector_size == 1);
    let sector_size = 512 in
    let size_sectors = Int64.(div size (of_int sector_size)) in
    let info = { read_write; sector_size; size_sectors } in
    Lwt.return (`Ok { server; client; s; info })

  let create file size =
    Img.create file size;
    connect file

  let disconnect { server; client; s } =
    let open Lwt.Infix in
    Nbd_lwt_unix.Client.disconnect client
    >>= fun () ->
    Lwt_unix.close s
    >>= fun () ->
    signal server Sys.sigterm;
    wait server;
    Lwt.return ()

end
