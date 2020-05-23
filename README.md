# influx-writer

An opinionated influxdb client in rust:

- http write requests are managed by a worker thread, so "writing" to influx is wait free on the hot path
- worker thread recovers from failed writes/requests with robust retry and failure handling
- library includes `measure!`, a mini-dsl for creating and sending measurements to influxdb

## `measure!`

`measure!` is a macro that allows highly ergonomic creation and sending of `OwnedMeasurement` instances, at the cost of a bit of a learning curve.

A single letter is used to denote the type of a field, and `t` is used to denote a tag:

```rust
measure!(influx, "measurement_name", t(color, "red"), i(n, 1), tm(now()));
```

In the above example, `influx` is the identifier for a `InfluxWriter` instance. `InfluxWriter` is a handle to the worker thread that is sending data to the influxdb server.

Now for a more advanced example: if only a single argument is passed to `t`, `f`, `i`, `b`, and other field type short-hands, it creates a field or tag by that name, and uses value in a variable binding of the same name as the field or tag's value:

```rust
let color = "red";
let n = 1;
let t = now(); // in this example, now() would return integer timestamp
measure!(influx, "even_faster", t(color), i(n), tm(t));
```

In the example above, `measure!` creates a `OwnedMeasurement` instance, fills it with a tag named "color" with the value "red", an integer field "n" with the value 1, and a timestamp with the value that's in the variable `t`. Then it uses `influx` to send the measurement to the worker thread.
