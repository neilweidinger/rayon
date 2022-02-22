use crate::latch::Latch;
use crate::registry::{
    Deque, DequeId, DequeState, Stealables, ThreadIndex, WorkerThread, XorShift64Star,
};
use crate::unwind;
use crossbeam_deque::{Injector, Steal};
use dashmap::DashMap;
use std::any::Any;
use std::cell::UnsafeCell;
use std::future::Future;
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

pub(super) enum JobResult<T> {
    None,
    Ok(T),
    Panic(Box<dyn Any + Send>),
}

pub(super) struct ExecutionContext<'a> {
    worker_thread: &'a WorkerThread,
    job_ref: JobRef,

    /// disable `Send` and `Sync`, just for a little future-proofing.
    _marker: PhantomData<*mut ()>,
}

impl<'a> ExecutionContext<'a> {
    pub(super) fn new(worker_thread: &'a WorkerThread, job_ref: JobRef) -> Self {
        Self {
            worker_thread,
            job_ref,
            _marker: PhantomData,
        }
    }
}

/// A `Job` is used to advertise work for other threads that they may
/// want to steal. In accordance with time honored tradition, jobs are
/// arranged in a deque, so that thieves can take from the top of the
/// deque while the main worker manages the bottom of the deque. This
/// deque is managed by the `thread_pool` module.
pub(super) trait Job {
    /// Unsafe: this may be called from a different thread than the one
    /// which scheduled the job, so the implementer must ensure the
    /// appropriate traits are met, whether `Send`, `Sync`, or both.
    unsafe fn execute(this: *const Self, context: ExecutionContext<'_>);
}

/// Effectively a Job trait object. Each JobRef **must** be executed
/// exactly once, or else data may leak.
///
/// Internally, we store the job's data in a `*const ()` pointer.  The
/// true type is something like `*const StackJob<...>`, but we hide
/// it. We also carry the "execute fn" from the `Job` trait.
#[derive(Copy, Clone)]
pub(super) struct JobRef {
    pointer: *const (),
    execute_fn: for<'a> unsafe fn(*const (), ExecutionContext<'a>),
}

unsafe impl Send for JobRef {}
unsafe impl Sync for JobRef {}

impl JobRef {
    /// Unsafe: caller asserts that `data` will remain valid until the
    /// job is executed.
    pub(super) unsafe fn new<T>(data: *const T) -> JobRef
    where
        T: Job,
    {
        let fn_ptr: for<'a> unsafe fn(*const T, ExecutionContext<'a>) = <T as Job>::execute;

        // erase types:
        JobRef {
            pointer: data as *const (),
            execute_fn: mem::transmute(fn_ptr),
        }
    }

    #[inline]
    pub(super) unsafe fn execute(context: ExecutionContext<'_>) {
        let execute_fn = context.job_ref.execute_fn;
        let job_pointer = context.job_ref.pointer;
        execute_fn(job_pointer, context);
    }
}

/// A job that will be owned by a stack slot. This means that when it
/// executes it need not free any heap data, the cleanup occurs when
/// the stack frame is later popped.  The function parameter indicates
/// `true` if the job was stolen -- executed on a different thread.
pub(super) struct StackJob<L, F, R>
where
    L: Latch + Sync,
    F: FnOnce(bool) -> R + Send, // TODO: refactor to use ExecutionContext
    R: Send,
{
    pub(super) latch: L,
    func: UnsafeCell<Option<F>>,
    result: UnsafeCell<JobResult<R>>,
    worker_thread_index: Option<ThreadIndex>, // the worker thread index on which this job was created, None if this job was injected
}

impl<L, F, R> StackJob<L, F, R>
where
    L: Latch + Sync,
    F: FnOnce(bool) -> R + Send,
    R: Send,
{
    pub(super) fn new(
        func: F,
        latch: L,
        worker_thread_index: Option<ThreadIndex>,
    ) -> StackJob<L, F, R> {
        StackJob {
            latch,
            func: UnsafeCell::new(Some(func)),
            result: UnsafeCell::new(JobResult::None),
            worker_thread_index,
        }
    }

    pub(super) unsafe fn as_job_ref(&self) -> JobRef {
        JobRef::new(self)
    }

    pub(super) unsafe fn run_inline(self, stolen: bool) -> R {
        self.func.into_inner().unwrap()(stolen)
    }

    pub(super) unsafe fn into_result(self) -> R {
        self.result.into_inner().into_return_value()
    }
}

impl<L, F, R> Job for StackJob<L, F, R>
where
    L: Latch + Sync,
    F: FnOnce(bool) -> R + Send,
    R: Send,
{
    unsafe fn execute(this: *const Self, context: ExecutionContext<'_>) {
        fn call<R>(func: impl FnOnce(bool) -> R, injected: bool) -> impl FnOnce() -> R {
            move || func(injected)
        }

        let this = &*this;
        let abort = unwind::AbortIfPanic;
        let func = (*this.func.get()).take().unwrap();
        let injected = if let Some(index) = this.worker_thread_index {
            index != context.worker_thread.index()
        } else {
            true
        };

        (*this.result.get()) =
            // If the injected parameter we receive is true, then this job has definitely
            // been injected and we pass this to the function. Otherwise, we fall back to
            // whatever boolean value this job was created with: (*this).injected.
            match unwind::halt_unwinding(call(func, injected)) {
                Ok(x) => JobResult::Ok(x),
                Err(x) => JobResult::Panic(x),
            };
        this.latch.set();
        mem::forget(abort);
    }
}

/// Represents a job stored in the heap. Used to implement
/// `scope`. Unlike `StackJob`, when executed, `HeapJob` simply
/// invokes a closure, which then triggers the appropriate logic to
/// signal that the job executed.
///
/// (Probably `StackJob` should be refactored in a similar fashion.)
pub(super) struct HeapJob<BODY>
where
    BODY: FnOnce() + Send,
{
    job: UnsafeCell<Option<BODY>>,
}

impl<BODY> HeapJob<BODY>
where
    BODY: FnOnce() + Send,
{
    pub(super) fn new(func: BODY) -> Self {
        HeapJob {
            job: UnsafeCell::new(Some(func)),
        }
    }

    /// Creates a `JobRef` from this job -- note that this hides all
    /// lifetimes, so it is up to you to ensure that this JobRef
    /// doesn't outlive any data that it closes over.
    pub(super) unsafe fn as_job_ref(self: Box<Self>) -> JobRef {
        let this: *const Self = mem::transmute(self);
        JobRef::new(this)
    }
}

impl<BODY> Job for HeapJob<BODY>
where
    BODY: FnOnce() + Send,
{
    unsafe fn execute(this: *const Self, _: ExecutionContext<'_>) {
        let this: Box<Self> = mem::transmute(this);
        let job = (*this.job.get()).take().unwrap();
        job();
    }
}

impl<T> JobResult<T> {
    /// Convert the `JobResult` for a job that has finished (and hence
    /// its JobResult is populated) into its return value.
    ///
    /// NB. This will panic if the job panicked.
    pub(super) fn into_return_value(self) -> T {
        match self {
            JobResult::None => unreachable!(),
            JobResult::Ok(x) => x,
            JobResult::Panic(x) => unwind::resume_unwinding(x),
        }
    }
}

// Represents top level task
pub(super) struct FutureJob<L, F>
where
    L: Latch + Sync,
    F: Future,
{
    pub(super) latch: L,
    future: UnsafeCell<F>,
    result: UnsafeCell<JobResult<F::Output>>,
    waker: UnsafeCell<Option<FutureJobWaker>>, // store waker *data* here on stack to avoid heap allocation
}

impl<L, F> FutureJob<L, F>
where
    L: Latch + Sync,
    F: Future,
{
    pub(super) fn new(future: F, latch: L) -> Self {
        Self {
            latch,
            future: UnsafeCell::new(future),
            result: UnsafeCell::new(JobResult::None),
            waker: UnsafeCell::new(None),
        }
    }

    pub(super) unsafe fn as_job_ref(&self) -> JobRef {
        JobRef::new(self)
    }

    pub(super) unsafe fn into_result(self) -> F::Output {
        self.result.into_inner().into_return_value()
    }
}

thread_local! {
    static RNG: XorShift64Star = XorShift64Star::new(); // TODO: thread local to avoid creating new rng on every FutureJob
                                                        // creation, is optimization really necessary?
}

#[derive(Clone)]
struct FutureJobWaker {
    stealables: *const Arc<Stealables>, // TODO: raw pointer to hide lifetime, this is just to avoid performance penalty of ref counting, is this really necessary
    deque_bench: *const DashMap<DequeId, Deque>, // TODO: raw pointer also to hide lifetime and avoid refcounts (although deque_bench isn't Arc'd to begin with)
    suspended_deque_id: DequeId,
    suspended_job_ref: JobRef,
}

impl FutureJobWaker {
    fn wake_by_ref(&self) {
        // Get the current (and soon to be replaced) state of the deque. Use this to later check if
        // deque needs to be moved into a stealable set or not.
        let stealables = unsafe { &*self.stealables };
        let (_, deque_state, stealable_set_index) = stealables
            .get_deque_stealer_info(self.suspended_deque_id)
            .expect("Stealer for suspended deque not found");

        assert!(
            deque_state == DequeState::Suspended,
            "Future triggered waker, but deque this future belongs to was not marked as suspended"
        );

        // This future was woken up, so push the ex-suspended and now non-suspended future JobRef
        // back onto the deque, so that another thread can steal it. Pushing the JobRef onto the
        // deque here when the future wakes up, as opposed to proactively pushing it when the
        // blocked future is first encountered, ensures that a deque only ever contains
        // non-suspended jobs, which simplifies the stealing implemenrtation.
        let deque_worker = &mut *(unsafe { &*self.deque_bench }
            .get_mut(&self.suspended_deque_id)
            .expect("Suspended deque not found in deque bench"));
        deque_worker.push(self.suspended_job_ref);

        // Mark suspended deque as resumable.
        stealables.update_deque_state(self.suspended_deque_id, DequeState::Resumable);

        // If deque not already in a stealable set, we must place it in some thread's stealable set.
        if stealable_set_index.is_none() {
            RNG.with(|rng| {
                let random_victim_index = rng.next_usize(stealables.get_num_threads());

                // Insert this ex-suspended and now resumable deque into a random victim's stealable set
                // Corresponds to markSuspendedResumable(future.suspended) in ProWS line 39
                stealables.add_deque_to_stealable_set(random_victim_index, self.suspended_deque_id);
            });
        }
    }

    const WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        FutureJobWaker::vtable_clone,
        FutureJobWaker::vtable_wake,
        FutureJobWaker::vtable_wake_by_ref,
        FutureJobWaker::vtable_drop,
    );

    unsafe fn vtable_clone(data: *const ()) -> RawWaker {
        RawWaker::new(data, &FutureJobWaker::WAKER_VTABLE)
    }

    unsafe fn vtable_wake(data: *const ()) {
        FutureJobWaker::wake_by_ref(&*(data as *const FutureJobWaker));
    }

    unsafe fn vtable_wake_by_ref(data: *const ()) {
        FutureJobWaker::wake_by_ref(&*(data as *const FutureJobWaker));
    }

    unsafe fn vtable_drop(_: *const ()) {
        // Do nothing here, as FutureJobWaker contains no resources
    }
}

impl<L, F> Job for FutureJob<L, F>
where
    L: Latch + Sync,
    F: Future,
{
    unsafe fn execute(this: *const Self, execution_context: ExecutionContext<'_>) {
        let this = &*this;
        let abort = unwind::AbortIfPanic;

        let worker_thread = execution_context.worker_thread;
        let stealables = execution_context.worker_thread.stealables();

        // Get the ID of the deque this FutureJob is in, since it may return pending and this deque
        // will become suspended, and the waker will need to know the ID of this deque in order to
        // mark it resumable
        let suspended_deque_id = { &*worker_thread.active_deque().get() }
            .as_ref()
            .expect("Worker thread executing a job erroneously has no active deque")
            .id();
        let future_job_waker = FutureJobWaker {
            stealables,
            suspended_deque_id,
            deque_bench: worker_thread.registry().deque_bench(),
            suspended_job_ref: execution_context.job_ref,
        };

        // Move future job waker data into this FutureJob so that it lives there
        *this.waker.get() = Some(future_job_waker);
        let future_job_waker_ptr =
            { &*this.waker.get() }.as_ref().unwrap() as *const FutureJobWaker;
        let waker = Waker::from_raw(RawWaker::new(
            future_job_waker_ptr as *const (),
            &FutureJobWaker::WAKER_VTABLE,
        ));
        let mut context = Context::from_waker(&waker);
        let pinned_future = Pin::new_unchecked(&mut *this.future.get());

        let execute_poll = || match pinned_future.poll(&mut context) {
            Poll::Ready(x) => {
                *(this.result.get()) = JobResult::Ok(x);
                this.latch.set();
            }
            Poll::Pending => {
                // Set active deque for worker thread to None so that thread steals on next round
                let active_deque = { &mut *worker_thread.active_deque().get() }
                    .take()
                    .expect("Worker thread executing a job erroneously has no active deque");

                // Remove deque from stealable set to prevent being stolen from; we will add to
                // another threads stealable set if we see the deque contains other non-suspended
                // jobs. If it does not and only contains the one suspended job, this deque will
                // not be found in any stealable set and as such will not be able to be stolen
                // from.
                stealables
                    .remove_deque_from_stealable_set(worker_thread.index(), active_deque.id());

                // Mark deque as suspended
                stealables.update_deque_state(active_deque.id(), DequeState::Suspended);

                // If the deque contains non-suspended jobs, it still contains work and can be
                // stolen from. As such add it to a random victim's stealable set.
                if active_deque.len() > 0 {
                    let random_victim_index = 0_usize; // TODO: make this actually random
                    stealables.add_deque_to_stealable_set(random_victim_index, active_deque.id());
                }

                // Move ex-active deque to the bench, so that it can live somewhere (the deque must
                // not be dropped, since it will be resumed again later when the future completes).
                // There should not already be a deque with the same ID in the bench, if so panic.
                let deque_id_already_existed = worker_thread
                    .registry()
                    .deque_bench()
                    .insert(active_deque.id(), active_deque);
                assert!(
                    deque_id_already_existed.is_none(),
                    "When moving active deque to bench, bench already contained a deque with the
                    same ID"
                );

                // TODO: should we put some sort of latch here that waker needs to wait on before
                // actually performing waking logic? So that deque suspension etc. guaranteed to
                // happen first
            }
        };

        if let Err(panic) = unwind::halt_unwinding(execute_poll) {
            *(this.result.get()) = JobResult::Panic(panic);
            this.latch.set();
        };

        mem::forget(abort);
    }
}

/// Indirect queue to provide FIFO job priority.
pub(super) struct JobFifo {
    inner: Injector<JobRef>,
}

impl JobFifo {
    pub(super) fn new() -> Self {
        JobFifo {
            inner: Injector::new(),
        }
    }

    pub(super) unsafe fn push(&self, job_ref: JobRef) -> JobRef {
        // A little indirection ensures that spawns are always prioritized in FIFO order.  The
        // jobs in a thread's deque may be popped from the back (LIFO) or stolen from the front
        // (FIFO), but either way they will end up popping from the front of this queue.
        self.inner.push(job_ref);
        JobRef::new(self)
    }
}

impl Job for JobFifo {
    unsafe fn execute(this: *const Self, mut context: ExecutionContext<'_>) {
        // We "execute" a queue by executing its first job, FIFO.
        loop {
            match (*this).inner.steal() {
                Steal::Success(job_ref) => {
                    context.job_ref = job_ref;
                    JobRef::execute(context);
                    break;
                }
                Steal::Empty => panic!("FIFO is empty"),
                Steal::Retry => {}
            }
        }
    }
}
