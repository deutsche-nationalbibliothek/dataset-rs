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
	$(CARGO) build --release --workspace

dev-install:
	$(CARGO) install --debug -q --path crates/datapod --bin datapod
	$(CARGO) install --debug -q --path . --bin dataset

install:
	install -Dm755 target/release/dataset $(DESTDIR)$(BINDIR)/dataset
	install -Dm755 target/release/datapod $(DESTDIR)$(BINDIR)/datapod

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/dataset
	rm -f $(DESTDIR)$(BINDIR)/datapod

.PHONY: build clean docs check test clippy check-fmt release install uninstall

