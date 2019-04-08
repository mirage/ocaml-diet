open Core
open Core_bench.Std

module IntDiet = Diet.Make(struct
    type t = int
    let compare (x: t) (y: t) = Pervasives.compare x y
    let zero = 0
    let succ x = x + 1
    let pred x = x - 1
    let add x y = x + y
    let sub x y = x - y
    let to_string = string_of_int
  end)

let state = Random.State.make_self_init ()

let fisher_yates_shuffle a =
  for i = Array.length a-1 downto 1 do
    let j = Random.State.int state (i + 1) in
    let tmp = a.(i) in
    a.(i) <- a.(j);
    a.(j) <- tmp;
  done

let gen_array size =
  let gen_interval i =
    let length = Random.State.int state 8 in
    IntDiet.Interval.make (10 * i) (10 * i + length)
  in
  Array.init size ~f:gen_interval

let diet_from_array arr =
  Array.fold
    ~init:IntDiet.empty
    ~f:(fun diet intvl -> IntDiet.add intvl diet)
    arr

let gen_equal_diets n =
  let intervals = gen_array n in
  let regular = diet_from_array intervals in
  fisher_yates_shuffle intervals;
  let shuffled = diet_from_array intervals in
  regular, shuffled

let gen_non_equal_diets n =
  let one = diet_from_array @@ gen_array n in
  let other = diet_from_array @@ gen_array n in
  one, other

let create_indexed_with_initialization ~name ~args f =
  Bench.Test.create_group ~name @@
    List.map args
      ~f:
        (fun size ->
           let name = Printf.sprintf "size %d" size in
           Bench.Test.create_with_initialization ~name (f size)
        )

let () =
  Command.run
    (Bench.make_command
       [ create_indexed_with_initialization ~name:"Equal" ~args:[10; 100; 1000]
           (fun size `init ->
              let d, d' = gen_equal_diets size in
              (fun () -> IntDiet.equal d d'))
       ; create_indexed_with_initialization ~name:"Not equal" ~args:[10; 100; 1000]
           (fun size `init ->
              let d, d' = gen_non_equal_diets size in
              (fun () -> ignore @@ IntDiet.equal d d'))
       ])
