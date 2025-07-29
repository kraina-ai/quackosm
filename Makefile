SHELL := /bin/bash
FILES_DIR = $(shell pwd)/files

install:
	pdm install -dG:all

docs:
	mkdocs serve --livereload -w quackosm

test:
	pytest --durations=20 --doctest-modules --doctest-continue-on-failure quackosm
	pytest --durations=20 tests

low-resources-test:
	docker build --progress plain -f tests/low_resources/Dockerfile -t quackosm-low-resources-test . && \
	docker run --rm --memory=2g -v "$(FILES_DIR):/app/files" \
		-e GITHUB_ACTION -e GITHUB_RUN_ID -e GITHUB_REF -e GITHUB_REPOSITORY \
		-e GITHUB_SHA -e GITHUB_HEAD_REF -e CODECOV_TOKEN  -e LAST_COMMIT_SHA \
		quackosm-low-resources-test /bin/bash /app/run_tests.sh

.PHONY: install docs test
