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

let size =
  let doc = Printf.sprintf "Virtual size of the qcow image" in
  Arg.(value & pos 0 int64 1024L & info [] ~doc)

let output =
  let doc = Printf.sprintf "Path to the output file." in
  Arg.(value & pos 1 string "test.raw" & info [] ~doc)

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

let create_cmd =
  let doc = "create a qcow-formatted data file" in
  let man = [
    `S "DESCRIPTION";
    `P "Create a qcow-formatted data file";
  ] @ help in
  Term.(ret(pure Impl.create $ size $ output)),
  Term.info "create" ~sdocs:_common_options ~doc ~man

let repair_cmd =
  let doc = "Regenerate the refcount table in an image" in
  let man = [
    `S "DESCRIPTION";
    `P "Regenerate the refcount table in an image to make it compliant with
    the spec. We normally avoid updating the refcount at runtime as a
    performance optimisation."
  ] @ help in
  Term.(ret(pure Impl.repair $ filename)),
  Term.info "repair" ~sdocs:_common_options ~doc ~man

let default_cmd =
  let doc = "manipulate virtual disks stored in qcow2 files" in
  let man = help in
  Term.(ret (pure (fun _ -> `Help (`Pager, None)) $ common_options_t)),
  Term.info "qcow-tool" ~version:"1.0.0" ~sdocs:_common_options ~doc ~man

let cmds = [info_cmd; copy_cmd; create_cmd; check_cmd; repair_cmd]

let _ =
  match Term.eval_choice default_cmd cmds with
  | `Error _ -> exit 1
  | _ -> exit 0
