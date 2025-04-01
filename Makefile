.DEFAULT_GOAL := build

MAKEFLAGS += -rR

CARGO ?= cargo

DESTDIR=
PREFIX=/usr/local
BINDIR=$(PREFIX)/bin

build:
	$(CARGO) build --workspace

clean:
	$(CARGO) clean

docs:
	$(CARGO) doc --no-deps --workspace

check:
	$(CARGO) check --workspace

test:
	$(CARGO) nextest run --workspace --no-fail-fast

clippy:
	$(CARGO) clippy --workspace --all-features

check-fmt:
	$(CARGO) fmt --all -- --check

release:
	$(CARGO) build --release --workspace --features performant

dev-dataset:
	$(CARGO) install -q --debug --path crates/dataset --bin dataset

dev-datashed:
	$(CARGO) install -q --debug --path crates/datashed --bin datashed

dev-install: dev-dataset dev-datashed

install:
	install -Dm755 target/release/datashed $(DESTDIR)$(BINDIR)/datashed
	install -Dm755 target/release/dataset $(DESTDIR)$(BINDIR)/dataset

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/datashed
	rm -f $(DESTDIR)$(BINDIR)/dataset

.PHONY: build clean docs check test clippy check-fmt release install uninstall

