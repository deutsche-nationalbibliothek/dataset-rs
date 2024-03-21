.DEFAULT_GOAL := build

MAKEFLAGS += -rR

CARGO ?= cargo

build:
	$(CARGO) build

check:
	$(CARGO) check

test:
	$(CARGO) test

clippy:
	$(CARGO) clippy

check-fmt:
	$(CARGO) fmt --all -- --check

.PHONY: build check test clippy check-fmt
