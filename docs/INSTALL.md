# Installation

This project's minimum supported `rustc` version is `1.77.0`.

To install the `dataset` binary from source:

```console
$ git clone https://github.com/nwagner84/dataset.git && cd dataset
$ make release
$ sudo make install
```
By default the binary is installed to `/usr/local/bin`. You can change
`PREFIX` or `DESTDIR` if you want to install into another directory:

```console
$ sudo make PREFIX=/usr install
```

In order to uninstall the binary, run the `uninstall` target:

```console
$ sudo make uninstall
```
