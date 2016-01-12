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

let debug fmt =
  Printf.ksprintf (fun s ->
      Printf.fprintf stderr "%s\n%!" s
    ) fmt

let read_lines oc =
  let rec aux acc =
    let line =
      try Some (input_line oc)
      with End_of_file -> None
    in
    match line with
    | Some l -> aux (l :: acc)
    | None   -> List.rev acc
  in
  aux []

let or_failwith = function
  | `Ok x -> x
  | `Error (`Msg m) -> failwith m

let ignore_output (_: (string list * string list)) = ()

type process = (in_channel * out_channel * in_channel) * string

let check_exit_status cmdline = function
  | Unix.WEXITED 0 -> `Ok ()
  | _ -> debug "%s failed" cmdline; `Error (`Msg cmdline)

let start cmd args : process =
  let args' = List.map (fun x -> "'" ^ (String.escaped x) ^ "'") args in
  let cmdline = Printf.sprintf "%s %s " cmd (String.concat " " args') in
  debug "%s" cmdline;
  Unix.open_process_full cmdline (Unix.environment ()), cmdline

let wait ((oc, ic, ec), cmdline) =
  let exit_status = Unix.close_process_full (oc, ic, ec) in
  or_failwith @@ check_exit_status cmdline exit_status

let run cmd args =
  let (oc, ic, ec), cmdline = start cmd args in
  let out = read_lines oc in
  let err = read_lines ec in
  wait ((oc, ic, ec), cmdline);
  out, err
