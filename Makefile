SHELL := /bin/bash

install:
	pdm install -dG:all

docs:
	mkdocs serve --livereload -w quackosm

test:
	pytest --durations=20 --doctest-modules --doctest-continue-on-failure quackosm
	pytest --durations=20 tests -n logical

.PHONY: install docs test
