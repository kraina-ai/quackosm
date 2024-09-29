FROM python:3.11-slim-bookworm

# install QuackOSM CLI
# since this is built from a slim base image, build-essential is required to compile python-geohash

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
 && pip install --no-cache-dir \
    quackosm[cli] \
 && apt-get purge -y --auto-remove \
    build-essential \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# purging build-essential saves ~290 MB in the final image

WORKDIR /data
VOLUME /data

ENTRYPOINT [ "quackosm" ]
CMD [ "--help" ]