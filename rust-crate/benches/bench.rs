use criterion::{Criterion, black_box, criterion_group, criterion_main};
use polars::prelude::*;
use rusty_cruncher::rollup_impl; // use internal for pure-Rust bench

fn make_df(n: usize) -> DataFrame {
    let groups: Vec<&str> = (0..n).map(|i| if i % 2 == 0 { "A" } else { "B" }).collect();
    let values: Vec<f64> = (0..n)
        .map(|i| (i as f64).sin() + 0.01 * (i as f64))
        .collect();
    df!(
    "service" => groups,
    "value" => values,
    )
    .unwrap()
}

fn bench_rollup(c: &mut Criterion) {
    let df = make_df(200_000);
    c.bench_function("rollup_impl_128", |b| {
        b.iter(|| {
            let out = rollup_impl(df.clone(), &vec!["service".into()], "value", 128, 3.0).unwrap();
            black_box(out);
        })
    });
}

criterion_group!(benches, bench_rollup);
criterion_main!(benches);
