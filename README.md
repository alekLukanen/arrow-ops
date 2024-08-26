# arrow-ops
Go implementation of common Apache Arrow operations such as comparing rows in two records,
sorting records, deduplicating records by a subset of columns, taking rows from one or more records,
checking for equality of records, and more. Historical benchmark results can be found in the `benchmakrResults/`
directory. These will be maintained to track performance improvements and regression issues over time.

A few of the functions here are inspired by the Rust implementation in the Apache Arrow compute
crate: https://arrow.apache.org/rust/arrow/compute/index.html but the implementation is not a direct
port. The functions here use slightly different patterns given that Go doesn't support many of the
language features that Rust has.

### Data Types

Not all data types are supported by this package. Any data type that is not supported will return an error
when used. Most common data types are supported.

### Running Benchmarks

You will need to have the `benchstat` tool installed to run the benchmarks. You can install it by running:
```bash
go install golang.org/x/perf/cmd/benchstat@latest
```

Once you have `benchstat` installed, you can run the benchmarks by running:
```bash
go run cmd/benchmarks/main.go -new
```
This command will generate a new benchmark result file in the `benchmarkResults/` directory and 
display a comparison of the new benchmark result to the previous benchmark result.
