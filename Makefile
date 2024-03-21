CARGO ?= cargo

check:
	$(CARGO) check

test:
	$(CARGO) test

.PHONY: check test
