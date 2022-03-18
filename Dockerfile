FROM rust:1.59.0-slim-bullseye AS build
ARG DEBIAN_FRONTEND=noninteractive

ADD . /app
WORKDIR /app
RUN apt-get update \
  && apt-get install -y pkg-config libssl-dev \
  && cargo build --release

FROM debian:bullseye-slim
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
  && apt-get install -y libssl1.1 ca-certificates \
  && rm -rf /var/lib/apt/lists/
COPY --from=build /app/target/release/obs-gitlab-runner /usr/local/bin/

ENTRYPOINT /usr/local/bin/obs-gitlab-runner
