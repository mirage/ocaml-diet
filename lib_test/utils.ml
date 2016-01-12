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

type process = int * (in_channel * out_channel * in_channel) * string

let check_exit_status cmdline = function
  | Unix.WEXITED 0 -> `Ok ()
  | Unix.WEXITED n -> debug "%s failed" cmdline; `Error (`Msg (cmdline ^ ": " ^ (string_of_int n)))

let start cmd args : process =
  let stdin_r, stdin_w = Unix.pipe () in
  let stdout_r, stdout_w = Unix.pipe () in
  let stderr_r, stderr_w = Unix.pipe () in
  let pid = Unix.create_process cmd (Array.of_list (cmd :: args)) stdin_r stdout_w stderr_w in
  Unix.close stdin_r;
  Unix.close stdout_w;
  Unix.close stderr_w;
  let ic = Unix.out_channel_of_descr stdin_w in
  let oc = Unix.in_channel_of_descr stdout_r in
  let ec = Unix.in_channel_of_descr stderr_r in
  pid, (oc, ic, ec), Printf.sprintf "%s %s" cmd (String.concat " " args)

let wait (pid, (oc, ic, ec), cmdline) =
  close_out ic;
  close_in oc;
  close_in ec;
  let _, exit_status = Unix.waitpid [] pid in
  or_failwith @@ check_exit_status cmdline exit_status

let run cmd args =
  let pid, (oc, ic, ec), cmdline = start cmd args in
  let out = read_lines oc in
  let err = read_lines ec in
  wait (pid, (oc, ic, ec), cmdline);
  out, err
