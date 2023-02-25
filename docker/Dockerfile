FROM python:3.10-alpine

ARG REPO_DIR

EXPOSE 8000

ENV KASPAD_HOST_1=n.seeder1.kaspad.net:16110
ENV SQL_URI=postgresql+asyncpg://postgres:password@postgresql:5432/postgres
ARG version
ENV VERSION=$version

RUN apk --no-cache add \
  git \
  gcc \
  libc-dev \
  build-base \
  linux-headers \
  libpq-dev \
  dumb-init

RUN pip install \
  pipenv

RUN addgroup -S -g 55746 kaspa-db-filler \
  && adduser -h /app -S -D -g '' -G kaspa-db-filler -u 55746 kaspa-db-filler

WORKDIR /app

USER api

COPY --chown=api:api . /app

RUN pipenv install --deploy

ENTRYPOINT ["/usr/bin/dumb-init", "--"]

CMD pipenv run python main.py
