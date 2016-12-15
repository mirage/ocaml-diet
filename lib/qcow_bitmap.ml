(*
 * Copyright (C) 2016 David Scott <dave@recoil.org>
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

type t = {
  buf: Cstruct.t;
  len: int;
}

type elt = int64

type interval = elt * elt

let make len =
  let bytes_required = (len + 7) / 8 in
  let buf = Cstruct.create bytes_required in
  Cstruct.memset buf 0;
  { buf; len }

let set t n v =
  if n >= (Int64.of_int t.len) then invalid_arg (Printf.sprintf "Qcow_bitmap.set %Ld >= %d" n t.len);
  let i = Int64.(to_int (div n 8L)) in
  let byte = Cstruct.get_uint8 t.buf i in
  let byte' =
    if v
    then byte lor (1 lsl (Int64.(to_int (rem n 8L))))
    else byte land (lnot (1 lsl (Int64.(to_int (rem n 8L))))) in
  Cstruct.set_uint8 t.buf i byte'

let get t n =
  if n >= (Int64.of_int t.len) then invalid_arg (Printf.sprintf "Qcow_bitmap.get %Ld >= %d" n t.len);
  let i = Int64.(to_int (div n 8L)) in
  let byte = Cstruct.get_uint8 t.buf i in
  byte land (1 lsl (Int64.(to_int (rem n 8L)))) <> 0

let set' t n v =
  if n >= t.len then invalid_arg (Printf.sprintf "Qcow_bitmap.set %d >= %d" n t.len);
  let i = n / 8 in
  let byte = Cstruct.get_uint8 t.buf i in
  let byte' =
    if v
    then byte lor (1 lsl (n mod 8))
    else byte land (lnot (1 lsl (n mod 8))) in
  Cstruct.set_uint8 t.buf i byte'

let get' t n =
  if n >= t.len then invalid_arg (Printf.sprintf "Qcow_bitmap.get %d >= %d" n t.len);
  let i = n / 8 in
  let byte = Cstruct.get_uint8 t.buf i in
  byte land (1 lsl (n mod 8)) <> 0

module Interval = struct
  let make x y =
    if x > y then invalid_arg "Interval.make";
    x, y
  let x = fst
  let y = snd
end

let add (a, b) t =
  for i = Int64.to_int a to Int64.to_int b do
    set' t i true
  done

let remove (a, b) t =
  for i = Int64.to_int a to Int64.to_int b do
    set' t i false
  done

let min_elt t =
  let rec loop from =
    if get' t from then from else loop (from + 1) in
  try
    Int64.of_int @@ loop 0
  with _ -> raise Not_found

(* fold over the maximal contiguous intervals *)
let fold f t acc =
  let rec loop acc from =
    (* find a true element *)
    let rec find from v =
      if get' t from = v then from else find (from + 1) v in
    match find from true with
      | exception Invalid_argument _ ->
        (* there are no more *)
        acc
      | a ->
        (* find a false element, up to the end of the set *)
        let b = match find a false with
          | b -> b
          | exception Invalid_argument _ -> t.len in
        let acc = f (Int64.of_int a, Int64.of_int (b - 1)) acc in
        loop acc b in
  loop acc 0

(* fold over the maximal contiguous intervals *)
let fold_s f t acc =
  let open Lwt.Infix in
  let rec loop acc from =
    (* find a true element *)
    let rec find from v =
      if get' t from = v then from else find (from + 1) v in
    match find from true with
      | exception Invalid_argument _ ->
        (* there are no more *)
        Lwt.return acc
      | a ->
        (* find a false element, up to the end of the set *)
        let b = match find a false with
          | b -> b
          | exception Invalid_argument _ -> t.len in
        f (Int64.of_int a, Int64.of_int (b - 1)) acc
        >>= fun acc ->
        loop acc b in
  loop acc 0

(* fold over individual elements *)
let fold_individual f t acc =
  let range (from, upto) acc =
    let rec loop acc x =
      if x = (Int64.succ upto) then acc else loop (f x acc) (Int64.succ x) in
    loop acc from in
  fold range t acc

let elements t = fold_individual (fun x acc -> x :: acc) t [] |> List.rev

let to_string t =
  fold (fun (a, b) acc -> Printf.sprintf "%Ld - %Ld\n" a b :: acc) t []
  |> String.concat ", "

open Sexplib.Std
module Int = struct
  type t = int [@@deriving sexp]
  let compare (x: t) (y: t) = Pervasives.compare x y
end
module IntSet = Set.Make(Int)

module Test = struct

  let make_random n m =
    let diet = make n in
    let rec loop set = function
      | 0 -> set, diet
      | m ->
        let r = Random.int n in
        let i = Interval.make (Int64.of_int r) (Int64.of_int r) in
        let set, () =
          if Random.bool ()
          then IntSet.add r set, add i diet
          else IntSet.remove r set, remove i diet in
        loop set (m - 1) in
    loop IntSet.empty m

  let set_to_string set =
    String.concat "; " @@ List.map string_of_int @@ IntSet.elements set

  let check_equals set diet =
    let set' = IntSet.elements set |> List.map Int64.of_int in
    let diet' = elements diet in
    if set' <> diet' then begin
      (*
      Printf.fprintf stderr "Set contains: [ %s ]\n" @@ set_to_string set;
      Printf.fprintf stderr "Diet contains: [ %s ]\n" @@ diet_to_string diet;
      *)
      failwith "check_equals"
    end

  let test_adds () =
    for i = 1 to 1000 do
      let set, diet = make_random 1000 1000 in
      check_equals set diet;
    done

  let test_add_1 () =
    let t = make 10 in
    add (3L, 3L) t;
    add (3L, 4L) t;
    assert (elements t = [ 3L; 4L ])

  let test_remove_1 () =
    let t = make 10 in
    add (7L, 8L) t;
    remove (6L, 7L) t;
    assert (elements t = [ 8L ])

  let all = [
    "adding an element to the right", test_add_1;
    "removing an element on the left", test_remove_1;
    "adding and removing elements acts like a Set", test_adds;
  ]

end
