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

