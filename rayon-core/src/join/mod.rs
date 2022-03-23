use crate::job::{FutureJob, StackJob};
use crate::latch::SpinLatch;
use crate::registry;
use crate::registry::worker_thread::WorkerThread;
use crate::unwind;
use std::any::Any;
use std::future::Future;

use crate::FnContext;

#[cfg(test)]
mod test;

/// Takes two closures and *potentially* runs them in parallel. It
/// returns a pair of the results from those closures.
///
/// Conceptually, calling `join()` is similar to spawning two threads,
/// one executing each of the two closures. However, the
/// implementation is quite different and incurs very low
/// overhead. The underlying technique is called "work stealing": the
/// Rayon runtime uses a fixed pool of worker threads and attempts to
/// only execute code in parallel when there are idle CPUs to handle
/// it.
///
/// When `join` is called from outside the thread pool, the calling
/// thread will block while the closures execute in the pool.  When
/// `join` is called within the pool, the calling thread still actively
/// participates in the thread pool. It will begin by executing closure
/// A (on the current thread). While it is doing that, it will advertise
/// closure B as being available for other threads to execute. Once closure A
/// has completed, the current thread will try to execute closure B;
/// if however closure B has been stolen, then it will look for other work
/// while waiting for the thief to fully execute closure B. (This is the
/// typical work-stealing strategy).
///
/// # Examples
///
/// This example uses join to perform a quick-sort (note this is not a
/// particularly optimized implementation: if you **actually** want to
/// sort for real, you should prefer [the `par_sort` method] offered
/// by Rayon).
///
/// [the `par_sort` method]: ../rayon/slice/trait.ParallelSliceMut.html#method.par_sort
///
/// ```rust
/// # use rayon_core as rayon;
/// let mut v = vec![5, 1, 8, 22, 0, 44];
/// quick_sort(&mut v);
/// assert_eq!(v, vec![0, 1, 5, 8, 22, 44]);
///
/// fn quick_sort<T:PartialOrd+Send>(v: &mut [T]) {
///    if v.len() > 1 {
///        let mid = partition(v);
///        let (lo, hi) = v.split_at_mut(mid);
///        rayon::join(|| quick_sort(lo),
///                    || quick_sort(hi));
///    }
/// }
///
/// // Partition rearranges all items `<=` to the pivot
/// // item (arbitrary selected to be the last item in the slice)
/// // to the first half of the slice. It then returns the
/// // "dividing point" where the pivot is placed.
/// fn partition<T:PartialOrd+Send>(v: &mut [T]) -> usize {
///     let pivot = v.len() - 1;
///     let mut i = 0;
///     for j in 0..pivot {
///         if v[j] <= v[pivot] {
///             v.swap(i, j);
///             i += 1;
///         }
///     }
///     v.swap(i, pivot);
///     i
/// }
/// ```
///
/// # Warning about blocking I/O
///
/// The assumption is that the closures given to `join()` are
/// CPU-bound tasks that do not perform I/O or other blocking
/// operations. If you do perform I/O, and that I/O should block
/// (e.g., waiting for a network request), the overall performance may
/// be poor.  Moreover, if you cause one closure to be blocked waiting
/// on another (for example, using a channel), that could lead to a
/// deadlock.
///
/// # Panics
///
/// No matter what happens, both closures will always be executed.  If
/// a single closure panics, whether it be the first or second
/// closure, that panic will be propagated and hence `join()` will
/// panic with the same panic value. If both closures panic, `join()`
/// will panic with the panic value from the first closure.
pub fn join<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send,
{
    #[inline]
    fn call<R>(f: impl FnOnce() -> R) -> impl FnOnce(FnContext) -> R {
        move |_| f()
    }

    join_context(call(oper_a), call(oper_b))
}

/// Identical to `join`, except that the closures have a parameter
/// that provides context for the way the closure has been called,
/// especially indicating whether they're executing on a different
/// thread than where `join_context` was called. This will occur if
/// the second job is stolen by a different thread, or if
/// `join_context` was called from outside the thread pool to begin
/// with.
// TODO: should probably refactor to use ExecutionContext
pub fn join_context<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
where
    A: FnOnce(FnContext) -> RA + Send,
    B: FnOnce(FnContext) -> RB + Send,
    RA: Send,
    RB: Send,
{
    #[inline]
    fn call<R>(f: impl FnOnce(FnContext) -> R) -> impl FnOnce(bool) -> R {
        move |migrated| f(FnContext::new(migrated))
    }

    registry::in_worker(|worker_thread, injected| unsafe {
        // Create virtual wrapper for tasks; this all has to be
        // done here so that the stack frame can keep it all live
        // long enough.

        // if told we've been explicitly injected via the injected parameter when this closure is
        // called, override the worker thread index this job was created on and instead ensure that
        // our jobs will always have an execution injection context of true
        let worker_thread_index = if injected {
            None
        } else {
            Some(worker_thread.index())
        };

        let job_b = StackJob::new(
            call(oper_b),
            SpinLatch::new(worker_thread),
            worker_thread_index,
        );
        let job_b_ref = job_b.as_job_ref();
        worker_thread.push(job_b_ref);

        let job_a = StackJob::new(
            call(oper_a),
            SpinLatch::new(worker_thread),
            worker_thread_index,
        );
        let job_a_ref = job_a.as_job_ref();
        worker_thread.push(job_a_ref);

        worker_thread.wait_until(&job_a.latch); // Wait on job a latch first, since job a will be popped before job b and correspondingly have its latch set first
        worker_thread.wait_until(&job_b.latch);
        debug_assert!(job_a.latch.probe() && job_b.latch.probe());

        (job_a.into_result(), job_b.into_result())
    })
}

/// TODO: docs
pub fn spawn_blocking_future<A>(future: A) -> <A as Future>::Output
where
    A: Future + Send,
    <A as Future>::Output: Send,
{
    registry::in_worker(|worker_thread, _| unsafe {
        // Job lives here on stack, only after latch is set and we know job is completed does the stack get cleaned up.
        // Future gets moved into above mentioned job and lives there.
        // TODO: make pinning on stack more explicit using something like pin_mut! or something
        let job_latch = SpinLatch::new(worker_thread);
        let job = FutureJob::new(future, &job_latch);
        let job_ref = job.as_job_ref();
        worker_thread.push(job_ref);

        worker_thread.wait_until(job.latch); // Wait on job a latch first, since job a will be popped before job b and correspondingly have its latch set first
        debug_assert!(job.latch.probe());

        job.into_result()
    })
}

/// If job A panics, we still cannot return until we are sure that job
/// B is complete. This is because it may contain references into the
/// enclosing stack frame(s).
#[cold] // cold path
unsafe fn join_recover_from_panic(
    worker_thread: &WorkerThread,
    job_b_latch: &SpinLatch<'_>,
    err: Box<dyn Any + Send>,
) -> ! {
    worker_thread.wait_until(job_b_latch);
    unwind::resume_unwinding(err)
}
