(*
 * Copyright (C) 2018 Docker Inc
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

open Crowbar

module Int = struct
  type t = int
  let compare (x: t) (y: t) = Pervasives.compare x y
  let zero = 0
  let succ x = x + 1
  let pred x = x - 1
  let add x y = x + y
  let sub x y = x - y
  let to_string = string_of_int
end
module IntDiet = Diet.Make(Int)

(* Avoid max_int because the library needs to call succ, and we don't want it
   to wrap. *)
let positive_int = range (max_int - 1)

let interval = map [ positive_int; positive_int ] (fun a b ->
    if a <= b then IntDiet.Interval.make a b else IntDiet.Interval.make b a
)

let diet = fix (fun diet ->
    choose [
        const IntDiet.empty;
        map [ interval; diet ] (fun interval diet -> IntDiet.add interval diet);
    ]
)

let pp_diet = IntDiet.pp
let diet = with_printer pp_diet diet

(* FIXME: add equals / compare to the diet signature *)
let eq a b =
  let intervals t = IntDiet.fold (fun x acc -> x :: acc) t [] |> List.rev in
  intervals a = (intervals b)

let shuffle_a st a =
  for i = Array.length a-1 downto 1 do
    let j = Random.State.int st (i+1) in
    let tmp = a.(i) in
    a.(i) <- a.(j);
    a.(j) <- tmp;
  done

let check_equality interval_list rng_state =
  let state = Random.State.make (Array.of_list rng_state) in
  let interval_array = Array.of_list interval_list in
  let diet_of_array array =
    Array.fold_left (fun diet interval -> IntDiet.add interval diet) IntDiet.empty array
  in
  let diet1 = diet_of_array interval_array in
  shuffle_a state interval_array;
  let diet2 = diet_of_array interval_array in
  check (IntDiet.equal diet2 diet1)

let () =
  add_test ~name:"union is commutative" [diet; diet]
    (fun d1 d2 ->
       check_eq ~pp:pp_diet ~eq IntDiet.(union d1 d2) IntDiet.(union d2 d1));
  add_test ~name:"union is associative" [diet; diet; diet]
    (fun d1 d2 d3 ->
       check_eq ~pp:pp_diet ~eq IntDiet.(union d1 (union d2 d3)) IntDiet.(union (union d1 d2) d3));
  add_test ~name:"intersection is commutative" [diet; diet]
    (fun d1 d2 ->
       check_eq ~pp:pp_diet ~eq IntDiet.(inter d1 d2) IntDiet.(inter d2 d1));
  add_test ~name:"intersection is associative" [diet; diet; diet]
    (fun d1 d2 d3 ->
       check_eq ~pp:pp_diet ~eq IntDiet.(inter d1 (inter d2 d3)) IntDiet.(inter (inter d1 d2) d3));
  add_test ~name:"distributive 1" [diet; diet; diet]
    (fun d1 d2 d3 ->
       check_eq ~pp:pp_diet ~eq IntDiet.(union d1 (inter d2 d3)) IntDiet.(inter (union d1 d2) (union d1 d3)));
  add_test ~name:"distributive 2" [diet; diet; diet]
    (fun d1 d2 d3 ->
       check_eq ~pp:pp_diet ~eq IntDiet.(inter d1 (union d2 d3)) IntDiet.(union (inter d1 d2) (inter d1 d3)));
  add_test ~name:"equality" [list1 interval; list1 int] check_equality;
  ()
