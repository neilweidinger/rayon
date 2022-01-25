use rand::distributions::Standard;
use rand::prelude::*;

trait Joiner {
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

fn partition<T: Ord>(input: &mut [T]) -> usize {
    let pivot_index = input.len() - 1;
    let mut swap = 0;

    for i in 0..pivot_index {
        if input[i] <= input[pivot_index] {
            if swap != i {
                input.swap(swap, i);
            }

            swap += 1;
        }
    }

    if swap != pivot_index {
        input.swap(swap, pivot_index);
    }

    swap
}

fn quicksort<T: Ord + Send, J: Joiner>(input: &mut [T]) {
    if input.len() > 30 {
        // artificial latency incurring operation
        // if J::is_parallel() {
        //     std::thread::sleep(std::time::Duration::from_millis(1));
        // }

        if input.len() <= 10_000 && J::is_parallel() {
            return quicksort::<T, Serial>(input);
        }

        let mid = partition(input);
        let (left, right) = input.split_at_mut(mid);

        J::join(|| quicksort::<T, J>(left), || quicksort::<T, J>(right));
    } else {
        input.sort_unstable();
    }
}

enum Execution {
    Par,
    Serial,
}

fn sort_stuff(n: usize, exec: Execution) {
    let mut v: Vec<i32> = {
        let rng = rand::thread_rng();
        Standard.sample_iter(rng).take(n).collect()
    };

    println!("v: {:?}...", &v[0..10]);

    match exec {
        Execution::Par => quicksort::<_, Parallel>(&mut v),
        Execution::Serial => quicksort::<_, Serial>(&mut v),
    }

    println!("v: {:?}...{:?}", &v[0..5], &v[v.len() - 5..]);
}

fn main() {
    let mut args = std::env::args();

    if args.len() != 3 {
        eprintln!(r#"Usage: ./rayon_test n {{par|serial}}"#);
        return;
    }

    let n = args.nth(1).unwrap().parse().unwrap();
    let exec = args.next().unwrap();
    let exec = match exec.as_str() {
        "par" => Execution::Par,
        "serial" => Execution::Serial,
        _ => {
            eprintln!(r#"Usage: ./rayon_test n {{par|serial}}"#);
            return;
        }
    };

    sort_stuff(n, exec); // 100_000_000 good n to use
}
