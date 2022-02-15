use crate::job::{JobFifo, JobRef, StackJob};
use crate::latch::{AsCoreLatch, CoreLatch, CountLatch, Latch, LockLatch, SpinLatch};
use crate::log::Event::*;
use crate::log::Logger;
use crate::sleep::Sleep;
use crate::unwind;
use crate::{
    ErrorKind, ExitHandler, PanicHandler, StartHandler, ThreadPoolBuildError, ThreadPoolBuilder,
};
use crossbeam_deque::Worker as CrossbeamWorker;
use crossbeam_deque::{Injector, Steal, Stealer};
use dashmap::{DashMap, DashSet};
use std::any::Any;
use std::cell::{Cell, UnsafeCell};
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;
use std::iter::FromIterator;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr;
#[allow(deprecated)]
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::sync::{Arc, Once};
use std::thread;
use std::usize;

/// Thread builder used for customization via
/// [`ThreadPoolBuilder::spawn_handler`](struct.ThreadPoolBuilder.html#method.spawn_handler).
pub struct ThreadBuilder {
    name: Option<String>,
    stack_size: Option<usize>,
    active_deque: Deque,
    registry: Arc<Registry>,
    index: usize,
    stealable_deques: Arc<Vec<DashSet<DequeId>>>,
}

impl ThreadBuilder {
    /// Gets the index of this thread in the pool, within `0..num_threads`.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Gets the string that was specified by `ThreadPoolBuilder::name()`.
    pub fn name(&self) -> Option<&str> {
        self.name.as_ref().map(String::as_str)
    }

    /// Gets the value that was specified by `ThreadPoolBuilder::stack_size()`.
    pub fn stack_size(&self) -> Option<usize> {
        self.stack_size
    }

    /// Executes the main loop for this thread. This will not return until the
    /// thread pool is dropped.
    pub fn run(self) {
        unsafe {
            main_loop(
                self.active_deque,
                self.registry,
                self.index,
                self.stealable_deques,
            )
        }
    }
}

impl fmt::Debug for ThreadBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThreadBuilder")
            .field("pool", &self.registry.id())
            .field("index", &self.index)
            .field("name", &self.name)
            .field("stack_size", &self.stack_size)
            .finish()
    }
}

/// Generalized trait for spawning a thread in the `Registry`.
///
/// This trait is pub-in-private -- E0445 forces us to make it public,
/// but we don't actually want to expose these details in the API.
pub trait ThreadSpawn {
    private_decl! {}

    /// Spawn a thread with the `ThreadBuilder` parameters, and then
    /// call `ThreadBuilder::run()`.
    fn spawn(&mut self, thread: ThreadBuilder) -> io::Result<()>;
}

/// Spawns a thread in the "normal" way with `std::thread::Builder`.
///
/// This type is pub-in-private -- E0445 forces us to make it public,
/// but we don't actually want to expose these details in the API.
#[derive(Debug, Default)]
pub struct DefaultSpawn;

impl ThreadSpawn for DefaultSpawn {
    private_impl! {}

    fn spawn(&mut self, thread: ThreadBuilder) -> io::Result<()> {
        let mut b = thread::Builder::new();
        if let Some(name) = thread.name() {
            b = b.name(name.to_owned());
        }
        if let Some(stack_size) = thread.stack_size() {
            b = b.stack_size(stack_size);
        }
        b.spawn(|| thread.run())?;
        Ok(())
    }
}

/// Spawns a thread with a user's custom callback.
///
/// This type is pub-in-private -- E0445 forces us to make it public,
/// but we don't actually want to expose these details in the API.
#[derive(Debug)]
pub struct CustomSpawn<F>(F);

impl<F> CustomSpawn<F>
where
    F: FnMut(ThreadBuilder) -> io::Result<()>,
{
    pub(super) fn new(spawn: F) -> Self {
        CustomSpawn(spawn)
    }
}

impl<F> ThreadSpawn for CustomSpawn<F>
where
    F: FnMut(ThreadBuilder) -> io::Result<()>,
{
    private_impl! {}

    #[inline]
    fn spawn(&mut self, thread: ThreadBuilder) -> io::Result<()> {
        (self.0)(thread)
    }
}

pub(super) struct Registry {
    logger: Logger,
    thread_infos: Vec<ThreadInfo>,
    sleep: Sleep,
    injected_jobs: Injector<JobRef>,
    panic_handler: Option<Box<PanicHandler>>,
    start_handler: Option<Box<StartHandler>>,
    exit_handler: Option<Box<ExitHandler>>,
    deque_bench: DashSet<Deque>,
    deque_stealers: DashMap<DequeId, Stealer<JobRef>>,
    next_deque_id: AtomicUsize, // TODO: come up with better ID generation/handling

    // When this latch reaches 0, it means that all work on this
    // registry must be complete. This is ensured in the following ways:
    //
    // - if this is the global registry, there is a ref-count that never
    //   gets released.
    // - if this is a user-created thread-pool, then so long as the thread-pool
    //   exists, it holds a reference.
    // - when we inject a "blocking job" into the registry with `ThreadPool::install()`,
    //   no adjustment is needed; the `ThreadPool` holds the reference, and since we won't
    //   return until the blocking job is complete, that ref will continue to be held.
    // - when `join()` or `scope()` is invoked, similarly, no adjustments are needed.
    //   These are always owned by some other job (e.g., one injected by `ThreadPool::install()`)
    //   and that job will keep the pool alive.
    terminate_count: AtomicUsize,
}

/// ////////////////////////////////////////////////////////////////////////
/// Initialization

static mut THE_REGISTRY: Option<Arc<Registry>> = None;
static THE_REGISTRY_SET: Once = Once::new();

/// Starts the worker threads (if that has not already happened). If
/// initialization has not already occurred, use the default
/// configuration.
pub(super) fn global_registry() -> &'static Arc<Registry> {
    set_global_registry(|| Registry::new(ThreadPoolBuilder::new()))
        .or_else(|err| unsafe { THE_REGISTRY.as_ref().ok_or(err) })
        .expect("The global thread pool has not been initialized.")
}

/// Starts the worker threads (if that has not already happened) with
/// the given builder.
pub(super) fn init_global_registry<S>(
    builder: ThreadPoolBuilder<S>,
) -> Result<&'static Arc<Registry>, ThreadPoolBuildError>
where
    S: ThreadSpawn,
{
    set_global_registry(|| Registry::new(builder))
}

/// Starts the worker threads (if that has not already happened)
/// by creating a registry with the given callback.
fn set_global_registry<F>(registry: F) -> Result<&'static Arc<Registry>, ThreadPoolBuildError>
where
    F: FnOnce() -> Result<Arc<Registry>, ThreadPoolBuildError>,
{
    let mut result = Err(ThreadPoolBuildError::new(
        ErrorKind::GlobalPoolAlreadyInitialized,
    ));

    THE_REGISTRY_SET.call_once(|| {
        result = registry()
            .map(|registry: Arc<Registry>| unsafe { &*THE_REGISTRY.get_or_insert(registry) })
    });

    result
}

struct Terminator<'a>(&'a Arc<Registry>);

impl<'a> Drop for Terminator<'a> {
    fn drop(&mut self) {
        self.0.terminate()
    }
}

impl Registry {
    pub(super) fn new<S>(
        mut builder: ThreadPoolBuilder<S>,
    ) -> Result<Arc<Self>, ThreadPoolBuildError>
    where
        S: ThreadSpawn,
    {
        let n_threads = builder.get_num_threads();
        let breadth_first = builder.get_breadth_first();
        let deque_id = AtomicUsize::new(0);

        let deques: Vec<_> = (0..n_threads)
            .map(|_| {
                if breadth_first {
                    Deque::new_fifo(DequeId(deque_id.fetch_add(1, Ordering::Relaxed)))
                } else {
                    Deque::new_lifo(DequeId(deque_id.fetch_add(1, Ordering::Relaxed)))
                }
            })
            .collect();

        let deque_stealers =
            DashMap::from_iter(deques.iter().map(|deque| (deque.id, deque.stealer()))); // TODO: create with capacity? CachePadded??
        let logger = Logger::new(n_threads);

        let registry = Arc::new(Registry {
            logger: logger.clone(),
            thread_infos: (0..n_threads).map(|_| ThreadInfo::new()).collect(),
            sleep: Sleep::new(logger, n_threads),
            injected_jobs: Injector::new(),
            terminate_count: AtomicUsize::new(1),
            panic_handler: builder.take_panic_handler(),
            start_handler: builder.take_start_handler(),
            exit_handler: builder.take_exit_handler(),
            deque_bench: DashSet::<Deque>::new(), // TODO: make CachePadded?
            deque_stealers,
            next_deque_id: deque_id,
        });

        // If we return early or panic, make sure to terminate existing threads.
        let t1000 = Terminator(&registry);

        let stealable_deques: Vec<_> = deques
            .iter()
            .map(|deque| DashSet::<DequeId>::from_iter([deque.id]))
            .collect();

        let stealable_deques = Arc::new(stealable_deques);

        for (index, deque) in deques.into_iter().enumerate() {
            let thread = ThreadBuilder {
                name: builder.get_thread_name(index),
                stack_size: builder.get_stack_size(),
                registry: registry.clone(),
                active_deque: deque,
                index,
                stealable_deques: stealable_deques.clone(),
            };
            if let Err(e) = builder.get_spawn_handler().spawn(thread) {
                return Err(ThreadPoolBuildError::new(ErrorKind::IOError(e)));
            }
        }

        // Returning normally now, without termination.
        mem::forget(t1000);

        Ok(registry.clone())
    }

    pub(super) fn current() -> Arc<Registry> {
        unsafe {
            let worker_thread = WorkerThread::current();
            if worker_thread.is_null() {
                global_registry().clone()
            } else {
                (*worker_thread).registry.clone()
            }
        }
    }

    /// Returns the number of threads in the current registry.  This
    /// is better than `Registry::current().num_threads()` because it
    /// avoids incrementing the `Arc`.
    pub(super) fn current_num_threads() -> usize {
        unsafe {
            let worker_thread = WorkerThread::current();
            if worker_thread.is_null() {
                global_registry().num_threads()
            } else {
                (*worker_thread).registry.num_threads()
            }
        }
    }

    /// Returns the current `WorkerThread` if it's part of this `Registry`.
    pub(super) fn current_thread(&self) -> Option<&WorkerThread> {
        unsafe {
            let worker = WorkerThread::current().as_ref()?;
            if worker.registry().id() == self.id() {
                Some(worker)
            } else {
                None
            }
        }
    }

    /// Returns an opaque identifier for this registry.
    pub(super) fn id(&self) -> RegistryId {
        // We can rely on `self` not to change since we only ever create
        // registries that are boxed up in an `Arc` (see `new()` above).
        RegistryId {
            addr: self as *const Self as usize,
        }
    }

    #[inline]
    pub(super) fn log(&self, event: impl FnOnce() -> crate::log::Event) {
        self.logger.log(event)
    }

    pub(super) fn num_threads(&self) -> usize {
        self.thread_infos.len()
    }

    pub(super) fn handle_panic(&self, err: Box<dyn Any + Send>) {
        match self.panic_handler {
            Some(ref handler) => {
                // If the customizable panic handler itself panics,
                // then we abort.
                let abort_guard = unwind::AbortIfPanic;
                handler(err);
                mem::forget(abort_guard);
            }
            None => {
                // Default panic handler aborts.
                let _ = unwind::AbortIfPanic; // let this drop.
            }
        }
    }

    /// Waits for the worker threads to get up and running.  This is
    /// meant to be used for benchmarking purposes, primarily, so that
    /// you can get more consistent numbers by having everything
    /// "ready to go".
    pub(super) fn wait_until_primed(&self) {
        for info in &self.thread_infos {
            info.primed.wait();
        }
    }

    /// Waits for the worker threads to stop. This is used for testing
    /// -- so we can check that termination actually works.
    #[cfg(test)]
    pub(super) fn wait_until_stopped(&self) {
        for info in &self.thread_infos {
            info.stopped.wait();
        }
    }

    /// ////////////////////////////////////////////////////////////////////////
    /// MAIN LOOP
    ///
    /// So long as all of the worker threads are hanging out in their
    /// top-level loop, there is no work to be done.

    /// Push a job into the given `registry`. If we are running on a
    /// worker thread for the registry, this will push onto the
    /// deque. Else, it will inject from the outside (which is slower).
    pub(super) fn inject_or_push(&self, job_ref: JobRef) {
        let worker_thread = WorkerThread::current();
        unsafe {
            if !worker_thread.is_null() && (*worker_thread).registry().id() == self.id() {
                (*worker_thread).push(job_ref);
            } else {
                self.inject(&[job_ref]);
            }
        }
    }

    /// Push a job into the "external jobs" queue; it will be taken by
    /// whatever worker has nothing to do. Use this is you know that
    /// you are not on a worker of this registry.
    pub(super) fn inject(&self, injected_jobs: &[JobRef]) {
        self.log(|| JobsInjected {
            count: injected_jobs.len(),
        });

        // It should not be possible for `state.terminate` to be true
        // here. It is only set to true when the user creates (and
        // drops) a `ThreadPool`; and, in that case, they cannot be
        // calling `inject()` later, since they dropped their
        // `ThreadPool`.
        debug_assert_ne!(
            self.terminate_count.load(Ordering::Acquire),
            0,
            "inject() sees state.terminate as true"
        );

        let queue_was_empty = self.injected_jobs.is_empty();

        for &job_ref in injected_jobs {
            self.injected_jobs.push(job_ref);
        }

        self.sleep
            .new_injected_jobs(usize::MAX, injected_jobs.len() as u32, queue_was_empty);
    }

    fn has_injected_job(&self) -> bool {
        !self.injected_jobs.is_empty()
    }

    fn pop_injected_job(&self, worker_index: usize) -> Option<JobRef> {
        loop {
            match self.injected_jobs.steal() {
                Steal::Success(job) => {
                    self.log(|| JobUninjected {
                        worker: worker_index,
                    });
                    return Some(job);
                }
                Steal::Empty => return None,
                Steal::Retry => {}
            }
        }
    }

    /// If already in a worker-thread of this registry, just execute `op`.
    /// Otherwise, inject `op` in this thread-pool. Either way, block until `op`
    /// completes and return its return value. If `op` panics, that panic will
    /// be propagated as well.  The second argument indicates `true` if injection
    /// was performed, `false` if executed directly.
    pub(super) fn in_worker<OP, R>(&self, op: OP) -> R
    where
        OP: FnOnce(&WorkerThread, bool) -> R + Send,
        R: Send,
    {
        unsafe {
            let worker_thread = WorkerThread::current();
            if worker_thread.is_null() {
                self.in_worker_cold(op)
            } else if (*worker_thread).registry().id() != self.id() {
                self.in_worker_cross(&*worker_thread, op)
            } else {
                // Perfectly valid to give them a `&T`: this is the
                // current thread, so we know the data structure won't be
                // invalidated until we return.
                op(&*worker_thread, false)
            }
        }
    }

    #[cold]
    unsafe fn in_worker_cold<OP, R>(&self, op: OP) -> R
    where
        OP: FnOnce(&WorkerThread, bool) -> R + Send,
        R: Send,
    {
        thread_local!(static LOCK_LATCH: LockLatch = LockLatch::new());

        LOCK_LATCH.with(|l| {
            // This thread isn't a member of *any* thread pool, so just block.
            // We can have this assert here since to have called in_worker_cold in the first place,
            // we must have checked to see if WorkerThread::current() is null earlier when
            // executing module::in_worker.
            debug_assert!(WorkerThread::current().is_null());
            let job = StackJob::new(
                |injected| {
                    let worker_thread = WorkerThread::current();
                    assert!(injected && !worker_thread.is_null());
                    op(&*worker_thread, injected)
                },
                l,
                true,
            );
            self.inject(&[job.as_job_ref()]);
            job.latch.wait_and_reset(); // Make sure we can use the same latch again next time.

            // flush accumulated logs as we exit the thread
            self.logger.log(|| Flush);

            job.into_result()
        })
    }

    #[cold]
    unsafe fn in_worker_cross<OP, R>(&self, current_thread: &WorkerThread, op: OP) -> R
    where
        OP: FnOnce(&WorkerThread, bool) -> R + Send,
        R: Send,
    {
        // This thread is a member of a different pool, so let it process
        // other work while waiting for this `op` to complete.
        debug_assert!(current_thread.registry().id() != self.id());
        let latch = SpinLatch::cross(current_thread);
        let job = StackJob::new(
            |injected| {
                let worker_thread = WorkerThread::current();
                assert!(injected && !worker_thread.is_null());
                op(&*worker_thread, injected)
            },
            latch,
            true,
        );
        self.inject(&[job.as_job_ref()]);
        current_thread.wait_until(&job.latch);
        job.into_result()
    }

    /// Increments the terminate counter. This increment should be
    /// balanced by a call to `terminate`, which will decrement. This
    /// is used when spawning asynchronous work, which needs to
    /// prevent the registry from terminating so long as it is active.
    ///
    /// Note that blocking functions such as `join` and `scope` do not
    /// need to concern themselves with this fn; their context is
    /// responsible for ensuring the current thread-pool will not
    /// terminate until they return.
    ///
    /// The global thread-pool always has an outstanding reference
    /// (the initial one). Custom thread-pools have one outstanding
    /// reference that is dropped when the `ThreadPool` is dropped:
    /// since installing the thread-pool blocks until any joins/scopes
    /// complete, this ensures that joins/scopes are covered.
    ///
    /// The exception is `::spawn()`, which can create a job outside
    /// of any blocking scope. In that case, the job itself holds a
    /// terminate count and is responsible for invoking `terminate()`
    /// when finished.
    pub(super) fn increment_terminate_count(&self) {
        let previous = self.terminate_count.fetch_add(1, Ordering::AcqRel);
        debug_assert!(previous != 0, "registry ref count incremented from zero");
        assert!(
            previous != std::usize::MAX,
            "overflow in registry ref count"
        );
    }

    /// Signals that the thread-pool which owns this registry has been
    /// dropped. The worker threads will gradually terminate, once any
    /// extant work is completed.
    pub(super) fn terminate(&self) {
        if self.terminate_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            for (i, thread_info) in self.thread_infos.iter().enumerate() {
                thread_info.terminate.set_and_tickle_one(self, i);
            }
        }
    }

    /// Notify the worker that the latch they are sleeping on has been "set".
    pub(super) fn notify_worker_latch_is_set(&self, target_worker_index: usize) {
        self.sleep.notify_worker_latch_is_set(target_worker_index);
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RegistryId {
    addr: usize,
}

struct ThreadInfo {
    /// Latch set once thread has started and we are entering into the
    /// main loop. Used to wait for worker threads to become primed,
    /// primarily of interest for benchmarking.
    primed: LockLatch,

    /// Latch is set once worker thread has completed. Used to wait
    /// until workers have stopped; only used for tests.
    stopped: LockLatch,

    /// The latch used to signal that terminated has been requested.
    /// This latch is *set* by the `terminate` method on the
    /// `Registry`, once the registry's main "terminate" counter
    /// reaches zero.
    ///
    /// NB. We use a `CountLatch` here because it has no lifetimes and is
    /// meant for async use, but the count never gets higher than one.
    terminate: CountLatch,
}

impl ThreadInfo {
    fn new() -> ThreadInfo {
        ThreadInfo {
            primed: LockLatch::new(),
            stopped: LockLatch::new(),
            terminate: CountLatch::new(),
        }
    }
}

/// ////////////////////////////////////////////////////////////////////////
/// WorkerThread identifiers

enum DequeState {
    Active,
    Suspended,
    Resumable,
    Muggable,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct DequeId(usize);

// TODO: put all deque stuff into seperate module
struct Deque {
    deque: CrossbeamWorker<JobRef>,
    id: DequeId,
    state: DequeState,
}

impl Deque {
    fn new_fifo(id: DequeId) -> Self {
        Self {
            deque: CrossbeamWorker::new_fifo(),
            id,
            state: DequeState::Active,
        }
    }

    fn new_lifo(id: DequeId) -> Self {
        Self {
            deque: CrossbeamWorker::new_lifo(),
            id,
            state: DequeState::Active,
        }
    }
}

impl PartialEq for Deque {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Deque {}

impl Hash for Deque {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

// TODO: keep Deref impls?
impl Deref for Deque {
    type Target = CrossbeamWorker<JobRef>;

    fn deref(&self) -> &Self::Target {
        &self.deque
    }
}

impl DerefMut for Deque {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.deque
    }
}

// TODO: absolutely need to make sure this is safe
unsafe impl Sync for Deque {}

pub(super) struct WorkerThread {
    /// the "worker" half of our local deque
    active_deque: UnsafeCell<Option<Deque>>,
    stealable_deques: Arc<Vec<DashSet<DequeId>>>,

    /// local queue used for `spawn_fifo` indirection
    fifo: JobFifo,

    index: usize,

    /// A weak random number generator.
    rng: XorShift64Star,

    registry: Arc<Registry>,
}

// This is a bit sketchy, but basically: the WorkerThread is
// allocated on the stack of the worker on entry and stored into this
// thread local variable. So it will remain valid at least until the
// worker is fully unwound. Using an unsafe pointer avoids the need
// for a RefCell<T> etc.
thread_local! {
    static WORKER_THREAD_STATE: Cell<*const WorkerThread> = Cell::new(ptr::null());
}

impl Drop for WorkerThread {
    fn drop(&mut self) {
        // Undo `set_current`
        WORKER_THREAD_STATE.with(|t| {
            assert!(t.get().eq(&(self as *const _)));
            t.set(ptr::null());
        });
    }
}

impl WorkerThread {
    /// Gets the `WorkerThread` index for the current thread; returns
    /// NULL if this is not a worker thread. This pointer is valid
    /// anywhere on the current thread.
    #[inline]
    pub(super) fn current() -> *const WorkerThread {
        WORKER_THREAD_STATE.with(Cell::get)
    }

    /// Sets `self` as the worker thread index for the current thread.
    /// This is done during worker thread startup.
    fn set_current(thread: *const WorkerThread) {
        WORKER_THREAD_STATE.with(|t| {
            assert!(t.get().is_null());
            t.set(thread);
        });
    }

    /// Returns the registry that owns this worker thread.
    #[inline]
    pub(super) fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    #[inline]
    pub(super) fn log(&self, event: impl FnOnce() -> crate::log::Event) {
        self.registry.logger.log(event)
    }

    /// Our index amongst the worker threads (ranges from `0..self.num_threads()`).
    #[inline]
    pub(super) fn index(&self) -> usize {
        self.index
    }

    #[inline]
    pub(super) fn push(&self, job: JobRef) {
        self.log(|| JobPushed { worker: self.index });
        let active_deque = unsafe { &mut *self.active_deque.get() }.as_mut().unwrap();
        let queue_was_empty = active_deque.is_empty();
        active_deque.push(job);
        self.registry
            .sleep
            .new_internal_jobs(self.index, 1, queue_was_empty);
    }

    #[inline]
    pub(super) unsafe fn push_fifo(&self, job: JobRef) {
        self.push(self.fifo.push(job));
    }

    #[inline]
    pub(super) fn local_deque_is_empty(&self) -> bool {
        let active_deque = unsafe { &mut *self.active_deque.get() }.as_mut().unwrap();
        active_deque.is_empty()
    }

    /// Attempts to obtain a "local" job -- typically this means
    /// popping from the top of the stack, though if we are configured
    /// for breadth-first execution, it would mean dequeuing from the
    /// bottom.
    #[inline]
    pub(super) fn take_local_job(&self) -> Option<JobRef> {
        let active_deque = unsafe { &mut *self.active_deque.get() }.as_mut().unwrap();
        let popped_job = active_deque.pop();

        if popped_job.is_some() {
            self.log(|| JobPopped { worker: self.index });
        }

        popped_job
    }

    /// Wait until the latch is set. Try to keep busy by popping and
    /// stealing tasks as necessary.
    #[inline]
    pub(super) fn wait_until<L: AsCoreLatch + ?Sized>(&self, latch: &L) {
        let latch = latch.as_core_latch();
        if !latch.probe() {
            self.wait_until_cold(latch);
        }
    }

    #[cold]
    fn wait_until_cold(&self, latch: &CoreLatch) {
        // the code below should swallow all panics and hence never
        // unwind; but if something does wrong, we want to abort,
        // because otherwise other code in rayon may assume that the
        // latch has been signaled, and that can lead to random memory
        // accesses, which would be *very bad*
        let abort_guard = unwind::AbortIfPanic;

        let mut idle_state = self.registry.sleep.start_looking(self.index, latch);

        // main scheduling loop takes place here!
        while !latch.probe() {
            // Try to find some work to do. We give preference first
            // to things in our local deque, then in other workers
            // deques, and finally to injected jobs from the
            // outside. The idea is to finish what we started before
            // we take on something new.

            let (job, injected) = if let Some(job) = self.take_local_job() {
                (Some(job), false)
            } else {
                // TODO: get Rustfmt to not format this so poorly
                (
                    self.steal()
                        .or_else(|| self.registry.pop_injected_job(self.index)),
                    true,
                )
            };

            if let Some(job) = job {
                self.registry.sleep.work_found(idle_state);
                unsafe {
                    self.execute(job, injected);
                }
                idle_state = self.registry.sleep.start_looking(self.index, latch);
            } else {
                self.registry
                    .sleep
                    .no_work_found(&mut idle_state, latch, || self.registry.has_injected_job())
            }
        }

        // If we were sleepy, we are not anymore. We "found work" --
        // whatever the surrounding thread was doing before it had to
        // wait.
        self.registry.sleep.work_found(idle_state);

        self.log(|| ThreadSawLatchSet {
            worker: self.index,
            latch_addr: latch.addr(),
        });
        mem::forget(abort_guard); // successful execution, do not abort
    }

    #[inline]
    pub(super) unsafe fn execute(&self, job: JobRef, injected: bool) {
        job.execute(injected);
    }

    /// Try to steal a single job and return it.
    ///
    /// This should only be done as a last resort, when there is no
    /// local work to do.
    fn steal(&self) -> Option<JobRef> {
        // we only steal when we don't have any work to do locally
        debug_assert!(self.local_deque_is_empty());

        // otherwise, try to steal
        let thread_infos = &self.registry.thread_infos.as_slice();
        let num_threads = thread_infos.len();
        if num_threads <= 1 {
            return None;
        }

        loop {
            let mut retry = false;
            let start = self.rng.next_usize(num_threads);
            let job = (start..num_threads)
                .chain(0..start)
                .filter(move |&i| i != self.index)
                .find_map(|victim_index| {
                    let victim_stealables = &self.stealable_deques[victim_index];

                    let random_index = match victim_stealables.len() {
                        0 => return None,
                        len => self.rng.next_usize(len),
                    };

                    // TODO: this iteration strategy of selecting a random deque is super inefficient, must change
                    // If we receive a None here, this means another thread stole and subsequently
                    // removed this victim deque before we did, and we must retry
                    // TODO: see if retrying impacts scheduling algorithm analysis, e.g. do we keep
                    // trying to steal from same victim thread or should we move on to another
                    // victim?
                    // TODO: random index selection based on stealable set size and deque id
                    // retrieval based on random index are not atomic, see if this actually matters
                    // in practice
                    let victim_deque_id = victim_stealables.iter().nth(random_index)?.clone();
                    let victim_deque_stealer =
                        self.registry.deque_stealers.get(&victim_deque_id)?;

                    match victim_deque_stealer.steal() {
                        Steal::Success(job) => {
                            self.log(|| JobStolen {
                                worker: self.index,
                                victim: victim_index,
                            });

                            // TODO: comment this out for now, no need to remove from stealable set
                            // yet while we still have not implemented deck switching
                            // TODO: we also need to ensure that the steal operation and this
                            // removal from the victim_stealables set are performed atomically (or
                            // look into scheduling algorithm analysis to see if this even matters)
                            // victim_stealables
                            //     .remove(&victim_deque_id) // TODO: could this remove cause cache issues?
                            //     .unwrap(); // TODO: verify unwraps or just remove

                            Some(job)
                        }
                        Steal::Empty => None,
                        Steal::Retry => {
                            retry = true;
                            None
                        }
                    }
                });

            if job.is_some() || !retry {
                return job;
            }
        }
    }

    pub(super) fn suspend_deque(&self) {
        let mut active_deque = unsafe { &mut *self.active_deque.get() }.take().unwrap();
        active_deque.state = DequeState::Suspended;

        if !active_deque.is_empty() {
            let random_index = 0;
            self.stealable_deques[random_index].insert(active_deque.id);
        }
    }
}

/// ////////////////////////////////////////////////////////////////////////

unsafe fn main_loop(
    deque: Deque,
    registry: Arc<Registry>,
    index: usize,
    stealable_deques: Arc<Vec<DashSet<DequeId>>>,
) {
    // this is the only time a WorkerThread is created: this struct represents the state for the
    // current thread running the main loop function, and is used in another functions to retrieve
    // the currently running thread (currently running thread stored in thread local
    // WORKER_THREAD_STATE)
    let worker_thread = &WorkerThread {
        active_deque: UnsafeCell::new(Some(deque)),
        fifo: JobFifo::new(),
        index,
        rng: XorShift64Star::new(),
        registry: registry.clone(),
        stealable_deques,
    };
    WorkerThread::set_current(worker_thread);

    // let registry know we are ready to do work
    registry.thread_infos[index].primed.set();

    // Worker threads should not panic. If they do, just abort, as the
    // internal state of the threadpool is corrupted. Note that if
    // **user code** panics, we should catch that and redirect.
    let abort_guard = unwind::AbortIfPanic;

    // Inform a user callback that we started a thread.
    if let Some(ref handler) = registry.start_handler {
        let registry = registry.clone();
        match unwind::halt_unwinding(|| handler(index)) {
            Ok(()) => {}
            Err(err) => {
                registry.handle_panic(err);
            }
        }
    }

    let my_terminate_latch = &registry.thread_infos[index].terminate;
    worker_thread.log(|| ThreadStart {
        worker: index,
        terminate_addr: my_terminate_latch.as_core_latch().addr(),
    });
    worker_thread.wait_until(my_terminate_latch); // enter main scheduling loop

    // Should not be any work left in our queue.
    debug_assert!(worker_thread.take_local_job().is_none());

    // let registry know we are done
    registry.thread_infos[index].stopped.set();

    // Normal termination, do not abort.
    mem::forget(abort_guard);

    worker_thread.log(|| ThreadTerminate { worker: index });

    // Inform a user callback that we exited a thread.
    if let Some(ref handler) = registry.exit_handler {
        let registry = registry.clone();
        match unwind::halt_unwinding(|| handler(index)) {
            Ok(()) => {}
            Err(err) => {
                registry.handle_panic(err);
            }
        }
        // We're already exiting the thread, there's nothing else to do.
    }
}

/// If already in a worker-thread, just execute `op`.  Otherwise,
/// execute `op` in the default thread-pool. Either way, block until
/// `op` completes and return its return value. If `op` panics, that
/// panic will be propagated as well.  The second argument indicates
/// `true` if injection was performed, `false` if executed directly.
pub(super) fn in_worker<OP, R>(op: OP) -> R
where
    OP: FnOnce(&WorkerThread, bool) -> R + Send,
    R: Send,
{
    unsafe {
        let owner_thread = WorkerThread::current();
        if !owner_thread.is_null() {
            // Perfectly valid to give them a `&T`: this is the
            // current thread, so we know the data structure won't be
            // invalidated until we return.
            op(&*owner_thread, false)
        } else {
            global_registry().in_worker_cold(op) // This is where the global threadpool gets created
                                                 // when first calling something like join.
        }
    }
}

/// [xorshift*] is a fast pseudorandom number generator which will
/// even tolerate weak seeding, as long as it's not zero.
///
/// [xorshift*]: https://en.wikipedia.org/wiki/Xorshift#xorshift*
struct XorShift64Star {
    state: Cell<u64>,
}

impl XorShift64Star {
    fn new() -> Self {
        // Any non-zero seed will do -- this uses the hash of a global counter.
        let mut seed = 0;
        while seed == 0 {
            let mut hasher = DefaultHasher::new();
            #[allow(deprecated)]
            static COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;
            hasher.write_usize(COUNTER.fetch_add(1, Ordering::Relaxed));
            seed = hasher.finish();
        }

        XorShift64Star {
            state: Cell::new(seed),
        }
    }

    fn next(&self) -> u64 {
        let mut x = self.state.get();
        debug_assert_ne!(x, 0);
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state.set(x);
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Return a value from `0..n`.
    fn next_usize(&self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}
