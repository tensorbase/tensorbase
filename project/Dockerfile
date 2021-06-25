FROM ubuntu:20.04 as builder
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

COPY --from=rustlang/rust:nightly-buster-slim  /usr/local /usr/local

WORKDIR /root

# config local mirror if you are in China
# RUN sed -i s@/archive.ubuntu.com/@/mirrors.163.com/@g /etc/apt/sources.list
RUN apt-get update -y && apt-get dist-upgrade -y \
    && apt-get install clang cmake -y

COPY . /
# config local reg if you are in China
# RUN echo \
# "[source.crates-io]\n\
# registry = \"https://github.com/rust-lang/crates.io-index\"\n\
# replace-with = 'sjtu'\n\
# [source.sjtu]\n\
# registry = \"http://mirrors.sjtug.sjtu.edu.cn/git/crates.io-index\"\n" > $CARGO_HOME/config
RUN CARGO_PROFILE_RELEASE_LTO='thin' CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 cargo build --release