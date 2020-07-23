export MAKEFLAGS := "-j8"
export RUSTFLAGS := "-C link-arg=-fuse-ld=lld"

cargo +args='':
    cargo {{args}}

check +args='':
    @just cargo check {{args}}

build name +args='':
    @just cargo build --bin {{name}} {{args}}

release-build name +args='':
    @just cargo build --bin {{name}} --release {{args}}

example name +args='':
    @just cargo build --example {{name}} {{args}}

test +args='':
    @just cargo test {{args}}

doc +args='':
    @just cargo doc --open --document-private-items {{args}}


