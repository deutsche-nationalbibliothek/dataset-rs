# Dataset

[![CI](https://github.com/nwagner84/dataset/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/nwagner84/dataset/actions/workflows/ci.yml)

This project contains tools for creating datasets and for analyzing and
processing the [GND] (Integrated Authority File). These are developments
to support the research work in the Automated Indexing System project of
the [German National Library].

The tools are not recommended for productive use; no support is
provided.

## Installation

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


## Contributing

All contributors are required to "sign-off" their commits (using `git
commit -s`) to indicate that they have agreed to the [Developer
Certificate of Origin][DCO].


## License

This project is licensed under the terms of the [EUPL v1.2].


[GND]: https://gnd.network/
[German National Library]: https://www.dnb.de
[DCO]: https://developercertificate.org/
[German National Library]: https://www.dnb.de
[EUPL v1.2]: ./LICENSE
