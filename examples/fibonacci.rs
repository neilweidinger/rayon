use async_io::Timer;
use async_recursion::async_recursion;
use clap::Parser;
use std::time::Duration;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    n: u32,
    #[clap(short, long)]
    hide_latency: bool,
    #[clap(short, long)]
    latency_ms: u64,
}

trait Joiner {
    #[must_use]
    fn is_parallel() -> bool;

    fn join<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send;
}

struct Serial;

impl Joiner for Serial {
    #[must_use]
    fn is_parallel() -> bool {
        false
    }

    fn join<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        let ra = oper_a();
        let rb = oper_b();

        (ra, rb)
    }
}

struct Parallel;

impl Joiner for Parallel {
    #[must_use]
    fn is_parallel() -> bool {
        true
    }

    fn join<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        rayon::join(oper_a, oper_b)
    }
}

#[must_use]
fn fib<J: Joiner>(n: u32, latency: u64) -> (u32, u32) {
    if n <= 1 {
        return (n, 1);
    }

    // if n < 10 && J::is_parallel() {
    //     return fib::<Serial>(n); // cross over to serial execution
    // }

    // incur latency here
    std::thread::sleep(Duration::from_millis(latency));

    let (ra, rb) = J::join(|| fib::<J>(n - 1, latency), || fib::<J>(n - 2, latency));

    (ra.0 + rb.0, ra.1 + rb.1 + 1)
}

fn fib_latency(n: u32, latency: u64) -> (u32, u32) {
    if n <= 1 {
        return (n, 1);
    }

    let (ra, rb) = rayon::join_async(
        fib_latency_helper(n - 1, latency),
        fib_latency_helper(n - 2, latency),
    );

    (ra.0 + rb.0, ra.1 + rb.1 + 1)
}

#[async_recursion]
async fn fib_latency_helper(n: u32, latency: u64) -> (u32, u32) {
    if n <= 1 {
        return (n, 1);
    }

    // incur latency here
    Timer::after(Duration::from_millis(latency)).await;

    let (ra, rb) = rayon::join_async(
        fib_latency_helper(n - 1, latency),
        fib_latency_helper(n - 2, latency),
    );

    (ra.0 + rb.0, ra.1 + rb.1 + 1)
}

fn main() {
    let args = Args::parse();

    let (fib, calls) = if args.hide_latency {
        fib_latency(args.n, args.latency_ms)
    } else {
        fib::<Parallel>(args.n, args.latency_ms)
    };

    println!("result: {} calls: {}", fib, calls);
}
