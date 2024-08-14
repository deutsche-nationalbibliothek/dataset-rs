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
	$(CARGO) test --workspace

clippy:
	$(CARGO) clippy --workspace

check-fmt:
	$(CARGO) fmt --all -- --check

release:
	$(CARGO) build --release --workspace --features performant

dev-dataset:
	$(CARGO) install -q --debug --path crates/dataset --bin dataset

dev-datashed:
	$(CARGO) install -q --debug --path crates/datashed --bin datashed

dev-rdftab:
	$(CARGO) install -q --debug --path crates/rdf-tools --bin rdftab

dev-install: dev-dataset dev-datashed dev-rdftab

install:
	install -Dm755 target/release/datashed $(DESTDIR)$(BINDIR)/datashed
	install -Dm755 target/release/dataset $(DESTDIR)$(BINDIR)/dataset
	install -Dm755 target/release/rdftab $(DESTDIR)$(BINDIR)/rdftab

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/datashed
	rm -f $(DESTDIR)$(BINDIR)/dataset
	rm -f $(DESTDIR)$(BINDIR)/rdftab

.PHONY: build clean docs check test clippy check-fmt release install uninstall

