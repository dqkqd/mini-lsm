
test-compaction-simulator-w2d3-space:
  diff <(cargo run --bin compaction-simulator tiered --iterations 500)  <(cargo run --bin compaction-simulator-ref tiered --iterations 500)

test-compaction-simulator-w2d3-size-ratio:
  diff <(cargo run --bin compaction-simulator tiered --iterations 500 --size-only)  <(cargo run --bin compaction-simulator-ref tiered --iterations 500 --size-only)


all: test-compaction-simulator-w2d3-space test-compaction-simulator-w2d3-size-ratio


