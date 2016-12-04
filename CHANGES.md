0.6.0 (2016-12-04)
- rename ocamlfind package from `qcow-format` to `qcow` for uniformity
- add support for runtime configuration arguments to `connect` and `create`
- add support for `discard` (aka TRIM or UNMAP) and online compaction
  (through a stop-the-world GC)
- switch the build from `oasis` to `topkg` (thanks to @jgimenez)

0.5.0 (2016-11-26)
- `resize` now takes a new size in bytes (rather than sectors) and uses a
  labelled argument
- `qcow-tool info` now takes a `--filter <expression>` for example
  `qcow-tool info ... --filter .size` to view the virtual size

0.4.2 (2016-09-21)
- Don't break the build if `Block.connect` has optional arguments

0.4.1 (2016-08-17)
- Remove one necessary source of `flush` calls
- CLI: add `mapped` command to list the mapped regions of a file

0.4 (2016-08-03)
- For buffered block devices, call `flush` to guarantee metadata correctness
- In lazy_refcounts mode (the default), do not compute any refcounts
- CLI: the `repair` command should recompute refcounts

0.3 (2016-05-12)
- Depend on ppx, require OCaml 4.02+

0.2 (2016-01-15)
- Use qcow version 3 by default, setting `lazy_refcount=on`
- Unit tests now verify that `qemu-img check` is happy and that `qemu-nbd`
  sees the same data we wrote

0.1 (2015-11-09)
- initial `V1_LWT.BLOCK` support
- caches metadata for performance
- CLI tool for manipulating images
- supports the `seek_mapped` `seek_unmapped` interface for iterating over
  sparse regions

