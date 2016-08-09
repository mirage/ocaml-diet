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

let project_url = "http://github.com/djs55/ocaml-qcow"

open Cmdliner

(* Help sections common to all commands *)

let _common_options = "COMMON OPTIONS"
let help = [
 `S _common_options;
 `P "These options are common to all commands.";
 `S "MORE HELP";
 `P "Use `$(mname) $(i,COMMAND) --help' for help on a single command."; `Noblank;
 `S "BUGS"; `P (Printf.sprintf "Check bug reports at %s" project_url);
]

(* Options common to all commands *)
let common_options_t =
  let docs = _common_options in
  let debug =
    let doc = "Give only debug output." in
    Arg.(value & flag & info ["debug"] ~docs ~doc) in
  Term.(pure Common.make $ debug)

let filename =
  let doc = Printf.sprintf "Path to the qcow2 file." in
  Arg.(value & pos 0 file "test.qcow2" & info [] ~doc)

let kib = 1024L
let mib = Int64.mul kib 1024L
let gib = Int64.mul mib 1024L
let tib = Int64.mul gib 1024L
let pib = Int64.mul tib 1024L

let sizes = List.sort (fun (_, a) (_, b) -> compare a b) [
  "KiB", kib;
  "MiB", mib;
  "GiB", gib;
  "TiB", tib;
  "PiB", pib;
]

let size_parser txt =
  let endswith suffix txt =
    let suffix' = String.length suffix in
    let txt' = String.length txt in
    txt' >= suffix' && (String.sub txt (txt' - suffix') suffix' = suffix) in
  let prefix suffix txt =
    let suffix' = String.length suffix in
    let txt' = String.length txt in
    String.sub txt 0 (txt' - suffix') in
  try
    match List.fold_left (fun acc (suffix, multiplier) -> match acc with
      | Some x -> Some x
      | None when not(endswith suffix txt) -> None
      | None -> Some (Int64.(mul multiplier (of_string (prefix suffix txt))))
    ) None sizes with
    | None -> `Ok (Int64.of_string txt)
    | Some x -> `Ok x
  with Failure _ -> `Error ("invalid size: " ^ txt)

let size_printer ppf v =
  let txt =
    match List.fold_left (fun acc (suffix, multiplier) -> match acc with
      | Some x -> Some x
      | None when Int64.rem v multiplier = 0L -> Some (Int64.(to_string (div v multiplier) ^ suffix))
      | None -> None
    ) None sizes with
    | None -> Int64.to_string v
    | Some x -> x in
  Format.fprintf ppf "%s" txt

let size_converter = size_parser, size_printer

let size =
  let doc = Printf.sprintf "Virtual size of the qcow image" in
  Arg.(value & opt size_converter 1024L & info [ "size" ] ~doc)

let strict_refcounts =
  let doc = Printf.sprintf "Use strict (non-lazy) refcounts" in
  Arg.(value & flag & info [ "strict-refcounts" ] ~doc)

let output =
  let doc = Printf.sprintf "Path to the output file." in
  Arg.(value & pos 0 string "test.raw" & info [] ~doc)

let trace =
  let doc = Printf.sprintf "Print block device accesses for debugging" in
  Arg.(value & flag & info [ "trace" ] ~doc)

let info_cmd =
  let doc = "display general information about a qcow2" in
  let man = [
    `S "DESCRIPTION";
    `P "Display the contents of a qcow2 file header.";
  ] @ help in
  Term.(ret(pure Impl.info $ filename)),
  Term.info "info" ~sdocs:_common_options ~doc ~man

let check_cmd =
  let doc = "check the device for internal consistency" in
  let man = [
    `S "DESCRIPTION";
    `P "Scan through the device and check for internal consistency"
  ] @ help in
  Term.(ret(pure Impl.check $ filename)),
  Term.info "check" ~sdocs:_common_options ~doc ~man

let decode_cmd =
  let doc = "decode qcow2 formatted data and write a raw image" in
  let man = [
    `S "DESCRIPTION";
    `P "Decode qcow2 formatted data and write to a raw file.";
  ] @ help in
  Term.(ret(pure Impl.decode $ filename $ output)),
  Term.info "decode" ~sdocs:_common_options ~doc ~man

let encode_cmd =
  let doc = "Convert the file from raw to qcow2" in
  let man = [
    `S "DESCRIPTION";
    `P "Convert a raw file to qcow2 ."
  ] @ help in
  Term.(ret(pure Impl.encode $ filename $ output)),
  Term.info "encode" ~sdocs:_common_options ~doc ~man

let create_cmd =
  let doc = "create a qcow-formatted data file" in
  let man = [
    `S "DESCRIPTION";
    `P "Create a qcow-formatted data file";
  ] @ help in
  Term.(ret(pure Impl.create $ size $ strict_refcounts $ trace $ output)),
  Term.info "create" ~sdocs:_common_options ~doc ~man

let unsafe_buffering =
  let doc = Printf.sprintf "Run faster by caching writes in memory. A failure in the middle could corrupt the file." in
  Arg.(value & flag & info [ "unsafe-buffering" ] ~doc)

let repair_cmd =
  let doc = "Regenerate the refcount table in an image" in
  let man = [
    `S "DESCRIPTION";
    `P "Regenerate the refcount table in an image to make it compliant with
    the spec. We normally avoid updating the refcount at runtime as a
    performance optimisation."
  ] @ help in
  Term.(ret(pure Impl.repair $ unsafe_buffering $ filename)),
  Term.info "repair" ~sdocs:_common_options ~doc ~man

let sector =
  let doc = Printf.sprintf "Virtual sector within the qcow2 image" in
  Arg.(value & opt int64 0L & info [ "sector" ] ~doc)

let text =
  let doc = Printf.sprintf "Test to write into the qcow2 image" in
  Arg.(value & opt string "" & info [ "text" ] ~doc)

let write_cmd =
  let doc = "Write a string to a virtual address in a qcow2 image" in
  let man = [
    `S "DESCRIPTION";
    `P "Write a string at a given virtual sector offset in the qcow2 image."
  ] @ help in
  Term.(ret(pure Impl.write $ filename $ sector $ text $ trace)),
  Term.info "write" ~sdocs:_common_options ~doc ~man

let length =
  let doc = Printf.sprintf "Length of the data in 512-byte sectors" in
  Arg.(value & opt int64 1L & info [ "length" ] ~doc)

let read_cmd =
  let doc = "Read a string from a virtual address in a qcow2 image" in
  let man = [
    `S "DESCRIPTION";
    `P "Read a string at a given virtual sector offset in the qcow2 image."
  ] @ help in
  Term.(ret(pure Impl.read $ filename $ sector $ length $ trace)),
  Term.info "read" ~sdocs:_common_options ~doc ~man

let default_cmd =
  let doc = "manipulate virtual disks stored in qcow2 files" in
  let man = help in
  Term.(ret (pure (fun _ -> `Help (`Pager, None)) $ common_options_t)),
  Term.info "qcow-tool" ~version:"1.0.0" ~sdocs:_common_options ~doc ~man

let cmds = [info_cmd; create_cmd; check_cmd; repair_cmd; encode_cmd; decode_cmd;
  write_cmd; read_cmd]

let _ =
  Logs.set_reporter (Logs_fmt.reporter ());
  match Term.eval_choice default_cmd cmds with
  | `Error _ -> exit 1
  | _ -> exit 0
