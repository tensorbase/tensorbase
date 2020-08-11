FROM archlinux as builder
RUN pacman -Syyu base-devel wget git clang cmake --noconfirm

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN set -eux; \
    \
    url="https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init"; \
    wget "$url"; \
    chmod +x rustup-init; \
    ./rustup-init -y --no-modify-path --default-toolchain nightly; \
    rm rustup-init; \
    chmod -R a+w $RUSTUP_HOME $CARGO_HOME; \
    rustup --version; \
    cargo --version; \
    rustc --version;

COPY . /
RUN cargo build --release

FROM archlinux
COPY --from=builder /target/release/baseops /target/release/baseshell /base/
COPY --from=builder /target/release/conf /base/
COPY --from=builder /target/release/deps/libbasejitc.so /usr/lib/
CMD ["/base/baseops"]
