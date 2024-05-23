.DEFAULT_GOAL := build

MAKEFLAGS += -rR

CARGO ?= cargo

DESTDIR=
PREFIX=/usr/local
BINDIR=$(PREFIX)/bin

build:
	$(CARGO) build

clean:
	$(CARGO) clean

docs:
	$(CARGO) doc --no-deps

check:
	$(CARGO) check

test:
	$(CARGO) test

clippy:
	$(CARGO) clippy

check-fmt:
	$(CARGO) fmt --all -- --check

release:
	$(CARGO) build --features performant --release

dev-install:
	cargo install --path .

install:
	install -Dm755 target/release/dataset $(DESTDIR)$(BINDIR)/dataset

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/dataset

.PHONY: build clean docs check test clippy check-fmt release install uninstall

