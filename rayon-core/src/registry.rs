use crate::job::{ExecutionContext, JobFifo, JobRef, StackJob};
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
use dashmap::DashMap;
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;
use std::iter::FromIterator;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr;
#[allow(deprecated)]
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::sync::{Arc, Mutex, Once};
use std::thread;
use std::usize;

/// Thread builder used for customization via
/// [`ThreadPoolBuilder::spawn_handler`](struct.ThreadPoolBuilder.html#method.spawn_handler).
pub struct ThreadBuilder {
    name: Option<String>,
    stack_size: Option<usize>,
    active_deque: Deque,
    registry: Arc<Registry>,
    index: ThreadIndex,
    stealables: Arc<Stealables>,
    set_to_active_lock: Arc<Mutex<()>>,
}

impl ThreadBuilder {
    /// Gets the index of this thread in the pool, within `0..num_threads`.
    pub fn index(&self) -> ThreadIndex {
        self.index
    }

    /// Gets the string that was specified by `ThreadPoolBuilder::name()`.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
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
                self.stealables,
                self.set_to_active_lock,
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
    deque_bench: DashMap<DequeId, Deque>, // Centralized location where all non-active deques live
    next_deque_id: AtomicUsize,           // TODO: come up with better ID generation/handling
    breadth_first: bool,

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
    #[must_use]
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
            deque_bench: DashMap::new(), // TODO: make CachePadded?
            next_deque_id: deque_id,
            breadth_first,
        });

        let stealables = Arc::new(deques.iter().collect::<Stealables>());
        let set_to_active_lock = Arc::new(Mutex::new(()));

        // If we return early or panic, make sure to terminate existing threads.
        let t1000 = Terminator(&registry);

        for (index, deque) in deques.into_iter().enumerate() {
            let thread = ThreadBuilder {
                name: builder.get_thread_name(index),
                stack_size: builder.get_stack_size(),
                registry: registry.clone(),
                active_deque: deque,
                index,
                stealables: stealables.clone(),
                set_to_active_lock: set_to_active_lock.clone(),
            };

            if let Err(e) = builder.get_spawn_handler().spawn(thread) {
                return Err(ThreadPoolBuildError::new(ErrorKind::IOError(e)));
            }
        }

        // Returning normally now, without termination.
        mem::forget(t1000);

        Ok(registry.clone())
    }

    #[must_use]
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
    #[must_use]
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
    #[must_use]
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
    #[must_use]
    pub(super) fn id(&self) -> RegistryId {
        // We can rely on `self` not to change since we only ever create
        // registries that are boxed up in an `Arc` (see `new()` above).
        RegistryId {
            addr: self as *const Self as usize,
        }
    }

    #[inline]
    #[must_use]
    pub(super) fn deque_bench(&self) -> &DashMap<DequeId, Deque> {
        &self.deque_bench
    }

    fn next_deque_id(&self) -> usize {
        self.next_deque_id.fetch_add(1, Ordering::Relaxed)
    }

    fn breadth_first(&self) -> bool {
        self.breadth_first
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
                handler(err);
            }
            None => {
                panic!("Default panic handler");
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

    pub(super) fn wake_any_worker_thread(&self) {
        self.sleep.new_injected_jobs(usize::MAX, 1, true); // TODO: not sure what we should use as queue_was_empty arg
    }

    fn has_injected_job(&self) -> bool {
        !self.injected_jobs.is_empty()
    }

    fn pop_injected_job(&self, worker_index: ThreadIndex) -> Option<JobRef> {
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
                None,
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
            None,
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
    pub(super) fn notify_worker_latch_is_set(&self, target_worker_index: ThreadIndex) {
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
    #[must_use]
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

#[derive(Copy, Clone, PartialEq, Eq)]
pub(super) enum DequeState {
    Active,
    Suspended,
    Resumable,
    Muggable,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(super) struct DequeId(usize);

// TODO: put all deque stuff into seperate module
pub(super) struct Deque {
    deque: CrossbeamWorker<JobRef>,
    id: DequeId,
}

impl Deque {
    #[must_use]
    fn new_fifo(id: DequeId) -> Self {
        Self {
            deque: CrossbeamWorker::new_fifo(),
            id,
        }
    }

    #[must_use]
    fn new_lifo(id: DequeId) -> Self {
        Self {
            deque: CrossbeamWorker::new_lifo(),
            id,
        }
    }

    #[must_use]
    #[inline]
    pub(super) fn id(&self) -> DequeId {
        self.id
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

// TODO: absolutely need to make sure this is safe. An alternative could be to wrap a Deque in an
// AtomicCell, but this would have performance overheads.
unsafe impl Sync for Deque {}

pub(super) type ThreadIndex = usize;
type Outcome = Result<(), ()>;

pub(super) struct Stealables {
    stealable_sets: Vec<StealableSet>,
    deque_stealers: DashMap<DequeId, (Stealer<JobRef>, DequeState, Option<ThreadIndex>)>, // ThreadIndex represents the thread
                                                                                          // index that has this deque in its
                                                                                          // stealable set (a deque can be unstealable
                                                                                          // and not belong in a stealable set so
                                                                                          // it has no ThreadIndex: when it is suspended
                                                                                          // and has no ready jobs; we still keep the
                                                                                          // stealer here in the mapping for
                                                                                          // implementation simplicity)
}

impl Stealables {
    /// Update the state of a deque. Checking deque state is only important in two places: when
    /// performing a steal and when the Future waker needs to know whether to insert a newly
    /// resumable deque into a victim thread's stealable set or not (the newly resumable deque may
    /// have been in the suspended state but still in a workers stealable set if the deque had
    /// non-suspended jobs, or it may have only contained the suspended job and not have been in
    /// any stealable set). Additionally, a suspended deque but with non-suspended jobs may have
    /// randomly been rebalanced to other stealable sets.
    ///
    /// The Future Waker needs a quick way to determine whether a deque is in a stealable set or
    /// not. TODO: finish this doc comment
    pub(super) fn update_deque_state(&self, deque_id: DequeId, deque_state: DequeState) {
        self.deque_stealers
            .get_mut(&deque_id)
            .expect("Deque ID should have been in stealables mapping, but was not found")
            .value_mut()
            .1 = deque_state;
    }

    #[must_use]
    pub(super) fn get_deque_stealer_info(
        &self,
        deque_id: DequeId,
    ) -> Option<(Stealer<JobRef>, DequeState, Option<ThreadIndex>)> {
        self.deque_stealers.get(&deque_id).map(|entry| {
            let (stealer, state, stealable_set_index) = &*entry;

            (
                stealer.clone(),
                *state,
                stealable_set_index.as_ref().copied(),
            )
        })
    }

    #[must_use]
    fn get_random_deque_id(&self, thread_index: ThreadIndex) -> Option<DequeId> {
        self.stealable_sets[thread_index].get_random_deque_id()
    }

    // TODO: this function doesn't really belong to Stealables, try and find a nicer way to get the
    // number of worker threads
    #[must_use]
    #[inline]
    pub(super) fn get_num_threads(&self) -> usize {
        self.stealable_sets.len()
    }

    pub(super) fn add_deque_to_stealable_set(&self, thread_index: ThreadIndex, deque_id: DequeId) {
        assert!(thread_index < self.stealable_sets.len());

        // TODO: create an entry in map if this is the first time encountering this deque
        // if deque_id not in map -> insert entry into map
        if !self.deque_stealers.contains_key(&deque_id) {
            todo!();
        }

        let t = &mut self
            .deque_stealers
            .get_mut(&deque_id)
            .expect("Deque ID should have been in stealables mapping, but was not found");
        let (_, _, stealable_set_index) = t.value_mut();

        assert!(
            stealable_set_index.is_none(),
            "Trying to add deque to a stealable set, but deque already in a stealable set"
        );

        // Make sure to keep track of which threads stealable set this deque is in
        *stealable_set_index = Some(thread_index);

        // Add deque to selected threads stealable set
        self.stealable_sets[thread_index].add_deque(deque_id);
    }

    /// Removes a deque from a threads stealable set. Note that if you want to *destroy* a deque
    /// (i.e. free the deque and clean up all resources associated with it), call
    /// [`Stealables::destroy_deque`] instead.
    pub(super) fn remove_deque_from_stealable_set(
        &self,
        thread_index: ThreadIndex,
        deque_id: DequeId,
    ) -> Outcome {
        // Make sure to keep track that this deque isn't found in any stealable set
        self.deque_stealers
            .get_mut(&deque_id)
            .expect("Deque ID should have been in stealables mapping, but was not found")
            .value_mut()
            .2 = None;

        // Remove deque from selected threads stealable set
        self.stealable_sets[thread_index].remove_deque(deque_id)
    }

    /// This method destroys the corresponding stealer and state information for a deque. This
    /// should only be called when a deque is being destroyed (i.e. freed). Basically, we no longer
    /// are going to use this deque, so we need to clean up our stealing resources. If you only
    /// want to remove a deque from a thread's *stealable set* instead without destroying the
    /// deque, use [`Stealables::remove_deque_from_stealable_set`].
    pub(super) fn destroy_deque(&self, thread_index: ThreadIndex, deque_id: DequeId) {
        // First, remove deque from the stealable set it is currently in
        let _ = self.remove_deque_from_stealable_set(thread_index, deque_id);

        // Next, remove all data associated with this deque, by letting things go out of scope
        // TODO: how to enforce order that deque must be removed from stealable set before having
        // its resources destroyed? Another thread must not try to steal a deque that is being
        // destroyed. Memory barriers?
        let _ = self
            .deque_stealers
            .remove(&deque_id)
            .expect("Deque stealer to be destroyed was not found");
    }

    /// Move a stealable deque from another random victim thread into this thread.
    // TODO: we need to make sure this doesn't move worker active deques
    fn rebalance_stealables(&self, thread_index: ThreadIndex) {
        let rebalance_closure = || -> Outcome {
            let victim_index = RNG.with(|rng| rng.next_usize(self.get_num_threads()));

            // No need to do anything if victim thread is same as this thread
            if thread_index == victim_index {
                return Ok(());
            }

            if let Some(stealable_deque) = self.stealable_sets[victim_index].get_random_deque_id() {
                // Move deque from victim thread stealable set to this thread stealable set
                self.remove_deque_from_stealable_set(victim_index, stealable_deque)?;
                self.add_deque_to_stealable_set(thread_index, stealable_deque);
            }

            Ok(())
        };

        let rebalance_attempts = 5; // TODO: totally arbitrary, find a better cap

        for _ in 0..rebalance_attempts {
            if let Ok(_) = rebalance_closure() {
                return;
            }
        }
    }
}

impl<'a> FromIterator<&'a Deque> for Stealables {
    /// Use only during creation of registry. This assumes that each deque in the iterator
    /// represents the one *active* deque per thread during creation of the registry.
    fn from_iter<I: IntoIterator<Item = &'a Deque>>(iter: I) -> Self {
        let deque_stealers = DashMap::new(); // TODO: create with capacity? CachePadded??
        let mut stealable_sets = Vec::new(); // TODO: create with capacity? CachePadded??

        for (index, deque) in iter.into_iter().enumerate() {
            deque_stealers.insert(deque.id, (deque.stealer(), DequeState::Active, Some(index)));
            stealable_sets.push(IntoIterator::into_iter([deque.id]).collect::<StealableSet>());
        }

        Self {
            stealable_sets,
            deque_stealers,
        }
    }
}

// Basically contains a set of stealable deque IDs
struct StealableSet {
    stealable_deque_ids: Mutex<(HashMap<DequeId, usize>, Vec<DequeId>, XorShift64Star)>, // TODO: maybe use RwLock?
}

impl StealableSet {
    /// Returns a random deque ID in this stealable set, otherwise None if it contains no deque
    /// ID's
    #[must_use]
    fn get_random_deque_id(&self) -> Option<DequeId> {
        let (_, v, rng) = &*self.stealable_deque_ids.lock().unwrap();

        if v.is_empty() {
            return None;
        }

        let random_index = rng.next_usize(v.len());
        Some(v[random_index])
    }

    fn add_deque(&self, deque_id: DequeId) {
        let (map, v, _) = &mut *self.stealable_deque_ids.lock().unwrap();
        v.push(deque_id);
        map.insert(deque_id, v.len() - 1);
    }

    fn remove_deque(&self, deque_id: DequeId) -> Outcome {
        let (map, v, _) = &mut *self.stealable_deque_ids.lock().unwrap();

        // Bail early if deque no longer in stealable set. This can happen if another thread beats
        // this thread in removing it, e.g. when rebalancing or stealing.
        let index = if let Some(index) = map.remove(&deque_id) {
            index
        } else {
            return Err(());
        };
        let deque_id = v.pop().expect(
            "When removing deque from this stealable set, stealable set erroneously empty somehow",
        );

        // only if deque we want to remove is *not* in the tail position of the vector do we need
        // to actually perform the tail swap maneuver
        if index < v.len() {
            v[index] = deque_id;
        }

        Ok(())
    }
}

impl FromIterator<DequeId> for StealableSet {
    fn from_iter<I: IntoIterator<Item = DequeId>>(iter: I) -> Self {
        let v: Vec<_> = iter.into_iter().collect();
        let mut map = HashMap::with_capacity(v.len());

        for (index, deque_id) in v.iter().enumerate() {
            map.insert(*deque_id, index);
        }

        Self {
            stealable_deque_ids: Mutex::new((map, v, XorShift64Star::new())),
        }
    }
}

pub(super) struct WorkerThread {
    /// A worker thread owns its active deque, but all other deques (including a workers stealable
    /// deques) are stored on the bench (owned by the registry). The alternative would be to hold a
    /// reference, but this wouldn't work since the bench could reallocate invalidating any
    /// references to inside of it.
    /// TODO: perhaps a worker thread could just hold a DequeId instead?
    active_deque: RefCell<Option<Deque>>,
    stealables: Arc<Stealables>,
    set_to_active_lock: Arc<Mutex<()>>,

    /// local queue used for `spawn_fifo` indirection
    fifo: JobFifo,

    index: ThreadIndex,

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
    static RNG: XorShift64Star = XorShift64Star::new();
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
    #[must_use]
    pub(super) fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// Our index amongst the worker threads (ranges from `0..self.num_threads()`).
    #[inline]
    #[must_use]
    pub(super) fn index(&self) -> ThreadIndex {
        self.index
    }

    #[inline]
    #[must_use]
    pub(super) fn active_deque(&self) -> &RefCell<Option<Deque>> {
        &self.active_deque
    }

    #[inline]
    #[must_use]
    pub(super) fn stealables(&self) -> &Arc<Stealables> {
        &self.stealables
    }

    #[inline]
    pub(super) fn log(&self, event: impl FnOnce() -> crate::log::Event) {
        self.registry.logger.log(event)
    }

    #[inline]
    pub(super) fn push(&self, job: JobRef) {
        self.log(|| JobPushed { worker: self.index });
        let r = &mut *self.active_deque.borrow_mut();
        let active_deque = r.as_mut().unwrap();
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
    pub(super) fn active_deque_is_empty(&self) -> bool {
        match self.active_deque.borrow_mut().as_mut() {
            Some(active_deque) => active_deque.is_empty(),
            None => true,
        }
    }

    /// Attempts to obtain a "local" job -- typically this means
    /// popping from the top of the stack, though if we are configured
    /// for breadth-first execution, it would mean dequeuing from the
    /// bottom.
    #[inline]
    #[must_use]
    pub(super) fn take_local_job(&self) -> Option<JobRef> {
        // possible that active_deque is None: this can happen when blocked future encountered
        let mut t = self.active_deque.borrow_mut();
        let active_deque = t.as_mut()?;
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
        let mut idle_state = self.registry.sleep.start_looking(self.index, latch);

        // main scheduling loop takes place here!
        while !latch.probe() {
            // Try to find some work to do. We give preference first
            // to things in our local deque, then in other workers
            // deques, and finally to injected jobs from the
            // outside. The idea is to finish what we started before
            // we take on something new.
            if let Some(job) = self
                .take_local_job()
                .or_else(|| self.steal())
                .or_else(|| self.registry.pop_injected_job(self.index))
            {
                self.registry.sleep.work_found(idle_state);
                unsafe {
                    self.execute(job);
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
    }

    #[inline]
    pub(super) unsafe fn execute(&self, job: JobRef) {
        JobRef::execute(ExecutionContext::new(self, job));
    }

    /// Try to steal a single job and return it.
    ///
    /// This should only be done as a last resort, when there is no
    /// local work to do.
    #[must_use]
    fn steal(&self) -> Option<JobRef> {
        // we only steal when we don't have any work to do locally
        debug_assert!(self.active_deque_is_empty());

        // otherwise, try to steal
        let thread_infos = &self.registry.thread_infos.as_slice();
        let num_threads = thread_infos.len();
        if num_threads <= 1 {
            return None;
        }

        let steal_attempts = 10; // TODO: totally arbitrary cap on steal attempts, find a way to find a
                                 // better cap. We need a cap since at some point we need to give
                                 // up on stealing and check the global injector queue, and
                                 // possible let this thread go to sleep if it really can't find
                                 // anything.

        // Attempt steal procedure steal_attempts times.
        // Any use of the question mark error propogation operator means that a deque is not
        // stealable (or no longer stealable, if another thread has changed the deque state), and
        // in these cases we just retry the steal procedure.
        (0..steal_attempts).find_map(|_| -> Option<JobRef> {
            let victim_thread = self.rng.next_usize(num_threads);
            let victim_deque_id = self.stealables.get_random_deque_id(victim_thread)?;

            let (deque_stealer, deque_state, _stealable_set_index) =
                self.stealables.get_deque_stealer_info(victim_deque_id)?;

            // There used to be an assert here asserting that stealable_set_index == victim_thread,
            // but this is not quite accurate as another thread could have moved this victim deque
            // to another stealable set in the meantime (e.g. when rebalancing). If this happens,
            // it should not a problem since our mugging operation (set_to_active()) should just
            // fail (i.e. we retry steal operation) if it cannot safely mug, and even if the deque
            // moved to another stealable set it should be safe to simply steal off the top of it.

            // If muggable, mug entire deque and set as active deque for thread
            if deque_state == DequeState::Muggable {
                // Attempt to mug this deque and set it as this threads active deque. This could
                // fail, however, if another thread chooses to mug this exact deque as well but
                // beats this thread to setting it as its active deque. In this case just retry the
                // steal procedure.
                return self.set_to_active(victim_deque_id, victim_thread);
            }

            match deque_stealer.steal() {
                Steal::Success(job) => {
                    self.log(|| JobStolen {
                        worker: self.index,
                        victim: victim_thread,
                    });

                    if deque_stealer.is_empty() {
                        // This deque is empty and has no jobs to be stolen from, so remove it from
                        // its stealable set
                        let _ = self
                            .stealables
                            .remove_deque_from_stealable_set(victim_thread, victim_deque_id);

                        // If the deque is not Suspended (Suspended deque can be empty but awaiting
                        // suspended future to awake) but is empty, we should destroy/free the
                        // deque since no thread can steal from this deque, and if it is an empty
                        // Active deque a stealing thread will allocate a fresh deque anyway
                        if deque_state != DequeState::Suspended {
                            // TODO: free deque
                            // TODO: potential deque recycling optimization
                        }

                        self.stealables.rebalance_stealables(victim_thread);
                    }
                    // Our victim deque is marked resumable and there are more jobs in the deque
                    // also waiting to be executed, so we mark this deque as muggable so that
                    // another thread can mug this entire deque in the future.
                    else if deque_state == DequeState::Resumable {
                        self.stealables
                            .update_deque_state(victim_deque_id, DequeState::Muggable);
                    }

                    // This worker thread that is trying to steal may not have an active deque if
                    // during the execution of its last job it encountered a blocked future (the
                    // worker thread active_deque is set to None so that here in the steal
                    // procedure we know to create a new deque)
                    if self.active_deque.borrow_mut().is_none() {
                        // TODO: potential deque recycling optimization
                        let new_active_deque = if self.registry().breadth_first() {
                            Deque::new_fifo(DequeId(self.registry().next_deque_id()))
                        } else {
                            Deque::new_lifo(DequeId(self.registry().next_deque_id()))
                        };

                        *self.active_deque.borrow_mut() = Some(new_active_deque);
                    }

                    // No need to push popped job on to active deque, we would pop this job off in
                    // the next scheduling round anyway so just return it directly here
                    Some(job)
                }
                // TODO: I'm not really sure what we should do here when the crossbeam stealer
                // returns Retry, for now I think we should just retry stealing procedure
                Steal::Empty | Steal::Retry => None,
            }
        })
    }

    /// This is only intended to be called from the steal procedure. Sets (i.e. mugs) the victim
    /// deque to be this worker thread's active deque. Pass in the victim thread index as well,
    /// since we need to rebalance stealables and remove the deque from the victim thread stealable
    /// set, and move it into this worker thread's stealable set. This method is ensured to be
    /// atomic by using a lock, since we don't want threads trying to set the same muggable deque
    /// to be their active deques.
    fn set_to_active(
        &self,
        victim_deque_id: DequeId,
        victim_thread: ThreadIndex,
    ) -> Option<JobRef> {
        // Enter protected atomic section
        // TODO: we should probably have a lock around the actual worker thread active_deque field
        // as oppposed to just making set_to_active atomic.
        let _guard = self.set_to_active_lock.lock();

        // We have entered the protected atomic section. It could be possible that another thread
        // has already mugged this deque before we had a chance, so quickly check the deque state
        // again and bail early if this deque is no longer muggable.
        let (_, deque_state, _) = self.stealables.get_deque_stealer_info(victim_deque_id)?;
        if deque_state != DequeState::Muggable {
            return None;
        }

        // Deque is already in some other threads stealable set, so we must rebalance stealables
        self.stealables.rebalance_stealables(victim_thread);

        // Remove deque from previous threads stealable set, and move into this worker threads
        // stealable set
        let _ = self
            .stealables
            .remove_deque_from_stealable_set(victim_thread, victim_deque_id);
        self.stealables
            .add_deque_to_stealable_set(self.index, victim_deque_id);

        self.stealables
            .update_deque_state(victim_deque_id, DequeState::Active);

        // We only ever try to steal when we have run out of jobs to pop off on our local currently
        // active deque, or we have polled a blocked future poll in the last scheduling round,
        // suspended that deque, and are now trying to find a new deque (hence the if let, since we
        // could have no active deque in this case). This means if we're ever stealing because
        // we've run out of jobs on our local active deque, our local active deque must be empty.
        // Thus, it serves no more use and needs to be freed/discarded/recycled. We also need to
        // remember to remove the corresponding stealer.
        // TODO: possible optimization where we don't free deques, but leave them allocated for
        // later use again (no idea if this would work).
        if let Some(active_deque) = self.active_deque.borrow_mut().take() {
            // Destroy current active deque stealable resources
            // TODO: deque stealable resources must be destroyed before the actual deque worker is
            // destroyed, to avoid threads trying to steal from this deque when it's worker no
            // longer exists. How can we enforce this? Using memory barriers?
            self.stealables.destroy_deque(self.index, active_deque.id);

            // active_deque goes out of scope and gets destroyed
        }

        // Mug this entire deque by setting it to be our active deque. We also move the muggable
        // deque from the deque bench to live in our active deque field (this is a deque whose
        // state is muggable (not active), so we know it cannot be another thread's active deque,
        // which means it must be living in the deque bench).
        let muggable_deque = self
            .registry()
            .deque_bench()
            .remove(&victim_deque_id)
            .expect("Could not find muggable deque in deque bench")
            .1;
        let popped_job = muggable_deque.pop();
        *self.active_deque.borrow_mut() = Some(muggable_deque);

        popped_job
    }
}

/// ////////////////////////////////////////////////////////////////////////

unsafe fn main_loop(
    deque: Deque,
    registry: Arc<Registry>,
    index: ThreadIndex,
    stealables: Arc<Stealables>,
    set_to_active_lock: Arc<Mutex<()>>,
) {
    // this is the only time a WorkerThread is created: this struct represents the state for the
    // current thread running the main loop function, and is used in another functions to retrieve
    // the currently running thread (currently running thread stored in thread local
    // WORKER_THREAD_STATE)
    let worker_thread = &WorkerThread {
        active_deque: RefCell::new(Some(deque)),
        set_to_active_lock,
        fifo: JobFifo::new(),
        index,
        rng: XorShift64Star::new(),
        registry: registry.clone(),
        stealables,
    };
    WorkerThread::set_current(worker_thread);

    // let registry know we are ready to do work
    registry.thread_infos[index].primed.set();

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
pub(super) struct XorShift64Star {
    state: Cell<u64>,
}

impl XorShift64Star {
    pub(super) fn new() -> Self {
        // Any non-zero seed will do -- this uses the hash of a global counter.
        let mut seed = 0;
        while seed == 0 {
            let mut hasher = DefaultHasher::new();
            #[allow(deprecated)]
            static COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;
            hasher.write_usize(COUNTER.fetch_add(1, Ordering::Relaxed));
            seed = hasher.finish();
        }

        Self {
            state: Cell::new(seed),
        }
    }

    pub(super) fn next(&self) -> u64 {
        let mut x = self.state.get();
        debug_assert_ne!(x, 0);
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state.set(x);
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Return a value from `0..n`.
    pub(super) fn next_usize(&self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}
