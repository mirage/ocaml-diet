#!/usr/bin/env ocaml
#use "topfind"
#require "topkg"
open Topkg

let () =
  Pkg.describe "qcow" @@ fun c ->
  Ok [ Pkg.mllib "lib/qcow.mllib";
       Pkg.bin "cli/main" ~dst:"qcow-tool";
       Pkg.test "lib_test/test" ]
