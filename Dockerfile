FROM rust:latest as build

WORKDIR /app

COPY . .

RUN apt-get update \
  && apt-get install -y libsasl2-dev

RUN cargo build --release

FROM ubuntu:latest

WORKDIR /app

RUN apt-get update \
  && apt-get install -y libssl-dev openssl libsasl2-dev \
  && apt-get upgrade -y \
  && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/release/kt .

ENTRYPOINT ["./kt"]
CMD [ "--producer", "--consumer", "-d", "1s" ]
