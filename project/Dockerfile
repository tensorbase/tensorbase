FROM archlinux as builder

# local mirrors for China
RUN curl 'https://www.archlinux.org/mirrorlist/?country=CN&protocol=https&ip_version=4' \
        | sed 's/^#Server/Server/' > /etc/pacman.d/mirrorlist \
 && curl 'https://raw.githubusercontent.com/archlinuxcn/mirrorlist-repo/master/archlinuxcn-mirrorlist' \
        | sed 's/^#Server/Server/' > /etc/pacman.d/archlinuxcn-mirrorlist
RUN pacman -Syyu make clang cmake --noconfirm

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

COPY --from=rustlang/rust:nightly-buster-slim  /usr/local /usr/local

COPY . /
# local reg for China
RUN echo "[source.crates-io]" > $CARGO_HOME/config \
    && echo "registry = \"https://github.com/rust-lang/crates.io-index\"" >> $CARGO_HOME/config \
    && echo "replace-with = 'sjtu'" >> $CARGO_HOME/config \
    && echo "[source.sjtu]" >> $CARGO_HOME/config \
    && echo "registry = \"http://mirrors.sjtug.sjtu.edu.cn/git/crates.io-index\"" >> $CARGO_HOME/config

# if run for non-avx2 arch, disable baseops
# RUN rm -rf crates/base_ops    
RUN cargo build --release


FROM archlinux
COPY --from=builder /target/release/baseops /target/release/baseshell /base/
COPY --from=builder /target/release/conf /base/conf/
COPY --from=builder /samples/data_bin /data/n3/data
COPY --from=builder /samples/schema_bin /data/n3/schema
COPY --from=builder /target/release/deps/libbasejitc.so /usr/lib/

CMD ["/base/baseshell"]