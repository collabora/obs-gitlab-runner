FROM rust:1.88.0-slim-bookworm AS build
ARG DEBIAN_FRONTEND=noninteractive

ADD . /app
WORKDIR /app
RUN apt-get update \
  && apt-get install -y pkg-config libssl-dev \
  && cargo build --release

FROM debian:bookworm-slim
ARG DEBIAN_FRONTEND=noninteractive

RUN groupadd --gid 1001 obs-gitlab-runner \
  && useradd \
    --uid 1001 --gid 1001 \
    --no-create-home --home-dir /app \
    obs-gitlab-runner

RUN apt-get update \
  && apt-get install -y libssl3 ca-certificates \
  && rm -rf /var/lib/apt/lists/
COPY --from=build /app/target/release/obs-gitlab-runner /usr/local/bin/

USER obs-gitlab-runner

ENTRYPOINT ["/usr/local/bin/obs-gitlab-runner"]
