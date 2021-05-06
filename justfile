export MAKEFLAGS := "-j8"
export RUSTFLAGS := "-C link-arg=-fuse-ld=lld"

cargo +args='':
    cargo {{args}}

check +args='':
    @just cargo check {{args}}

bench +args='':
    @just cargo bench {{args}}

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

# just rebuild docs, don't open browser page again
redoc +args='': 
    @just cargo doc {{args}}

publish +args='':
    @just cargo publish --registry mmcxi {{args}}

update +args='':
    @just cargo update {{args}}

# blow away build dir and start all over again
rebuild:
    just cargo clean
    just update
    just test

# display env variables that will be used for building
show-build-env:
    @ env | rg RUST --color never


