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
fn fib<J: Joiner>(n: u32) -> (u32, u32) {
    if n <= 1 {
        return (n, 1);
    }

    if n < 10 && J::is_parallel() {
        return fib::<Serial>(n); // cross over to serial execution
    }

    let (ra, rb) = J::join(|| fib::<J>(n - 1), || fib::<J>(n - 2));

    (ra.0 + rb.0, ra.1 + rb.1 + 1)
}

fn main() {
    let mut args = std::env::args();

    if args.len() > 2 {
        eprintln!(r#"Usage: ./rayon_test n"#);
        return;
    }

    let n = args.nth(1).unwrap_or("25".to_string()).parse().unwrap();
    let (fib, calls) = fib::<Parallel>(n);

    println!("result: {} calls: {}", fib, calls);
}
