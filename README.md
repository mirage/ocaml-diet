Ocaml support for Qcow2 images
==============================

[![Build Status](https://travis-ci.org/mirage/ocaml-qcow.png?branch=master)](https://travis-ci.org/mirage/ocaml-qcow) [![Coverage Status](https://coveralls.io/repos/mirage/ocaml-qcow/badge.png?branch=master)](https://coveralls.io/r/mirage/ocaml-qcow?branch=master)

Please read [the API documentation](https://mirage.github.io/ocaml-qcow/).

Features
--------

- supports `resize`
- exposes sparseness information

Limitations
-----------

- cluster size is fixed at 64-bits
- no support for snapshots
- the refcount table is not updated, so resulting images will require
  a repair before they can be used by other tools
