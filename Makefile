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

dev-install:
	$(CARGO) install -q --debug --path crates/datashed --bin datashed
	# $(CARGO) install --debug -q --path . --bin dataset

install:
	install -Dm755 target/release/datashed $(DESTDIR)$(BINDIR)/datashed
	# install -Dm755 target/release/dataset $(DESTDIR)$(BINDIR)/dataset

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/datashed
	# rm -f $(DESTDIR)$(BINDIR)/dataset

.PHONY: build clean docs check test clippy check-fmt release install uninstall

