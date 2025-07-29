FROM python:3.12-slim-bookworm


RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git \
 && pip install --no-cache-dir \
    pdm \
 && apt-get purge -y --auto-remove \
    build-essential \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml pylock.toml pdm.toml README.md ./
RUN pdm install --prod --skip post_install --no-self
RUN pdm add --skip post_install -dG test coverage codecov-cli

COPY quackosm quackosm

RUN ls -la

RUN pdm install --prod --skip post_install

COPY tests/low_resources .

RUN ls -la
CMD ["pdm", "run", "test.py"]
