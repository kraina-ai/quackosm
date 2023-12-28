SHELL := /bin/bash

install:
	pdm install -dG:all

docs:
	mkdocs serve --livereload -w quackosm

test:
	pytest -n auto

.PHONY: install docs test
