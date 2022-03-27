use crate::deque::{Deque, DequeId, DequeState, Stealables, ThreadIndex};
use crate::job::{ExecutionContext, JobFifo, JobRef};
use crate::latch::{AsCoreLatch, CoreLatch};
use crate::log::Event::*;
use crate::registry::{Registry, XorShift64Star};
use crate::unwind;
use crossbeam_deque::Steal;
use std::cell::{Cell, UnsafeCell};
use std::ptr;
use std::sync::{Arc, Mutex};

thread_local! {
    static RNG: XorShift64Star = XorShift64Star::new();
}

pub(crate) struct WorkerThread {
    /// A worker thread owns its active deque, but all other deques (including a workers stealable
    /// deques) are stored on the bench (owned by the registry). The alternative would be to hold a
    /// reference, but this wouldn't work since the bench could reallocate invalidating any
    /// references to inside of it.
    active_deque: UnsafeCell<Option<Deque>>,
    stealables: Arc<Stealables>,
    set_to_active_lock: Arc<Mutex<()>>,

    /// local queue used for `spawn_fifo` indirection
    fifo: JobFifo,

    index: ThreadIndex,

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
    pub(super) fn new(
        active_deque: UnsafeCell<Option<Deque>>,
        stealables: Arc<Stealables>,
        set_to_active_lock: Arc<Mutex<()>>,
        fifo: JobFifo,
        index: ThreadIndex,
        registry: Arc<Registry>,
    ) -> Self {
        Self {
            active_deque,
            stealables,
            set_to_active_lock,
            fifo,
            index,
            registry,
        }
    }
    /// Gets the `WorkerThread` index for the current thread; returns
    /// NULL if this is not a worker thread. This pointer is valid
    /// anywhere on the current thread.
    #[inline]
    pub(crate) fn current() -> *const WorkerThread {
        WORKER_THREAD_STATE.with(Cell::get)
    }

    /// Sets `self` as the worker thread index for the current thread.
    /// This is done during worker thread startup.
    pub(crate) fn set_current(thread: *const WorkerThread) {
        WORKER_THREAD_STATE.with(|t| {
            assert!(t.get().is_null());
            t.set(thread);
        });
    }

    /// Returns the registry that owns this worker thread.
    #[inline]
    #[must_use]
    pub(crate) fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// Our index amongst the worker threads (ranges from `0..self.num_threads()`).
    #[inline]
    #[must_use]
    pub(crate) fn index(&self) -> ThreadIndex {
        self.index
    }

    #[inline]
    #[must_use]
    pub(crate) fn active_deque(&self) -> &UnsafeCell<Option<Deque>> {
        if unsafe { &*self.active_deque.get() }.is_none() {
            self.create_new_active_deque();
        }

        &self.active_deque
    }

    #[inline]
    #[must_use]
    pub(crate) fn stealables(&self) -> &Arc<Stealables> {
        &self.stealables
    }

    #[inline]
    pub(crate) fn log(&self, event: impl FnOnce() -> crate::log::Event) {
        self.registry.logger.log(event)
    }

    #[inline]
    pub(crate) fn push(&self, job: JobRef) {
        if unsafe { &*self.active_deque.get() }.is_none() {
            self.create_new_active_deque();
        }

        let active_deque = unsafe { &mut *self.active_deque.get() }.as_mut().unwrap();
        let queue_was_empty = active_deque.is_empty();
        active_deque.push(job);

        self.stealables.add_existing_deque_to_stealable_set(
            None,
            self.index,
            unsafe { &*self.active_deque.get() }.as_ref().unwrap().id(),
        );

        self.log(|| JobPushed {
            worker: self.index,
            deque_id: active_deque.id(),
        });

        self.registry
            .sleep
            .new_internal_jobs(self.index, 1, queue_was_empty);
    }

    #[inline]
    pub(crate) unsafe fn push_fifo(&self, job: JobRef) {
        self.push(self.fifo.push(job));
    }

    #[inline]
    pub(crate) fn active_deque_is_empty(&self) -> bool {
        match unsafe { &mut *self.active_deque.get() }.as_mut() {
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
    pub(crate) fn take_local_job(&self) -> Option<JobRef> {
        // Possible that active_deque is None: this can happen when blocked future encountered
        let active_deque = unsafe { &mut *self.active_deque.get() }.as_mut()?;
        let popped_job = active_deque.pop();

        if popped_job.is_some() {
            self.log(|| JobPopped {
                worker: self.index,
                deque_id: active_deque.id(),
                jobs_remaining: active_deque.len(),
            });
        } else {
            self.log(|| JobPoppedFailed {
                worker: self.index,
                deque_id: active_deque.id(),
                jobs_remaining: active_deque.len(),
            });
        }

        popped_job
    }

    /// Wait until the latch is set. Try to keep busy by popping and
    /// stealing tasks as necessary.
    #[inline]
    pub(crate) fn wait_until<L: AsCoreLatch + ?Sized>(&self, latch: &L) {
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
                self.log(|| ThreadDidNotFindWork { worker: self.index });
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

        std::mem::forget(abort_guard); // successful execution, do not abort
    }

    #[inline]
    pub(crate) unsafe fn execute(&self, job: JobRef) {
        job.execute(ExecutionContext::new(self));
    }

    pub(crate) fn create_new_active_deque(&self) {
        assert!(
            (unsafe { &*self.active_deque.get() }).is_none(),
            "Creating new active deque, but worker thread already has an active deque"
        );

        // TODO: potential deque recycling optimization
        let new_active_deque = if self.registry().breadth_first() {
            Deque::new_fifo(DequeId::new(self.registry().next_deque_id()))
        } else {
            Deque::new_lifo(DequeId::new(self.registry().next_deque_id()))
        };

        // Add freshly created active deque to worker stealable set
        self.stealables.add_new_deque_to_stealable_set(
            self.index,
            &new_active_deque,
            DequeState::Active,
        );

        unsafe {
            *self.active_deque.get() = Some(new_active_deque);
        }
    }

    /// Try to steal a single job and return it.
    ///
    /// This should only be done as a last resort, when there is no
    /// local work to do.
    #[must_use]
    fn steal(&self) -> Option<JobRef> {
        /// This returns an iterator over deques to steal from. The first STEAL_ATTEMPTS many
        /// deques are randomly selected deques (randomly selecting a deque may possibly not return
        /// a stealable deque, so we just *attempt* to return STEAL_ATTEMPTS many deques). After
        /// STEAL_ATTEMPTS, if we haven't already successfully stolen by then, we attempt once to
        /// brute force search for a stealable deque. This brute force is done so that if all
        /// previous random steal attempts fail, our last attempt will search all stealable sets
        /// for a deque, guaranteeing that we will find one if such a deque exists. We do this
        /// because it's possible, since initial victim stealable set selection is random, that a
        /// deque with work to be executed never gets selected by any thread, the threads
        /// mistakenly think there is no work left and go to sleep, but now there is still that
        /// deque with work to be done but no one left to wake the threads up, and the program
        /// never terminates.
        fn steal_victims_iter(
            num_threads: usize,
            stealables: &Arc<Stealables>,
        ) -> impl Iterator<Item = (usize, ThreadIndex, DequeId)> + '_ {
            const STEAL_ATTEMPTS: usize = 3; // TODO: totally arbitrary cap on steal attempts, find a way to find a
                                             // better cap. We need a cap since at some point we need to give
                                             // up on stealing and check the global injector queue, and
                                             // possible let this thread go to sleep if it really can't find
                                             // anything.
            let mut steal_counter = 0;
            std::iter::from_fn(move || loop {
                steal_counter += 1;

                match steal_counter {
                    counter if counter > STEAL_ATTEMPTS + 1 => {
                        break None;
                    }
                    counter if counter == STEAL_ATTEMPTS + 1 => {
                        if let Some((victim_thread, victim_deque_id)) =
                            stealables.find_stealable_deque_id()
                        {
                            break Some((steal_counter - 1, victim_thread, victim_deque_id));
                        }
                    }
                    _ => {
                        let victim_thread = RNG.with(|rng| rng.next_usize(num_threads));

                        if let Some(victim_deque_id) = stealables.get_random_deque_id(victim_thread)
                        {
                            break Some((steal_counter - 1, victim_thread, victim_deque_id));
                        }
                    }
                }
            })
        }

        // we only steal when we don't have any work to do locally
        debug_assert!(self.active_deque_is_empty());

        // otherwise, try to steal
        let thread_infos = &self.registry.thread_infos.as_slice();
        let num_threads = thread_infos.len();

        // Attempt steal procedure steal_attempts times.
        // Any use of the question mark error propogation operator means that a deque is not
        // stealable (or no longer stealable, if another thread has changed the deque state), and
        // in these cases we just retry the steal procedure.
        steal_victims_iter(num_threads, self.stealables()).find_map(
            |(attempt, victim_thread, victim_deque_id)| -> Option<JobRef> {
                let (deque_stealer, deque_state, _) =
                    self.stealables.get_deque_stealer_info(victim_deque_id)?;

                self.log(|| JobStealAttempt {
                    attempt,
                    worker: self.index,
                    victim_thread,
                    victim_deque_id,
                    deque_state,
                });

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

                let stolen_job = deque_stealer.steal();

                // Right after we attempt to steal from the victim deque, we handle the case where
                // the victim deque is possibly empty. If the deque is not empty and the deque is
                // marked resumable this means there are more jobs in the deque also waiting to be
                // executed, so we mark this deque as muggable so that another thread can mug this
                // entire deque in the future.
                if !self
                    .stealables
                    .handle_empty_deque(self.index, victim_deque_id)
                    && deque_state == DequeState::Resumable
                {
                    // TODO: should we use a lock here?
                    self.stealables
                        .update_deque_state(None, victim_deque_id, DequeState::Muggable);
                }

                match stolen_job {
                    Steal::Success(job) => {
                        self.log(|| JobStolen {
                            attempt,
                            worker: self.index,
                            victim_thread,
                            victim_deque_id,
                            deque_state,
                        });

                        // This worker thread that is trying to steal may not have an active deque if
                        // during the execution of its last job it encountered a blocked future: the
                        // worker thread active_deque is set to None so that here in the steal
                        // procedure we know to create a new active deque. This can also happen
                        // during Rayon startup, since worker threads are only allocated active
                        // deques when jobs are first pushed.
                        if unsafe { &*self.active_deque.get() }.is_none() {
                            self.create_new_active_deque();
                        }

                        // No need to push popped job on to active deque, we would pop this job off in
                        // the next scheduling round anyway so just return it directly here
                        Some(job)
                    }
                    Steal::Empty => {
                        self.log(|| JobStolenFailEmpty {
                            attempt,
                            worker: self.index,
                            victim_thread,
                            victim_deque_id,
                            deque_state,
                        });

                        None
                    }
                    Steal::Retry => {
                        self.log(|| JobStolenFailRetry {
                            attempt,
                            worker: self.index,
                            victim_thread,
                            victim_deque_id,
                            deque_state,
                        });

                        None
                    }
                }
            },
        )
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
        let _guard = self.set_to_active_lock.lock();

        self.log(|| SettingToActive {
            worker: self.index,
            victim_thread,
            victim_deque_id,
        });

        // We have entered the protected atomic section. It could be possible that another thread
        // has already mugged this deque before we had a chance, so quickly check the deque state
        // again and bail early if this deque is no longer muggable.
        let (_, deque_state, _) = self.stealables.get_deque_stealer_info(victim_deque_id)?;
        if deque_state != DequeState::Muggable {
            return None;
        }

        // Deque is already in some other threads stealable set, so we must rebalance stealables
        self.stealables.rebalance_stealables(None, victim_thread);

        // Remove deque from previous threads stealable set, and move into this worker threads
        // stealable set
        let _ = self
            .stealables
            .remove_deque_from_stealable_set(None, victim_deque_id);
        self.stealables
            .add_existing_deque_to_stealable_set(None, self.index, victim_deque_id);

        self.stealables
            .update_deque_state(None, victim_deque_id, DequeState::Active);

        // We only ever try to steal when we have run out of jobs to pop off on our local currently
        // active deque, or we have polled a blocked future poll in the last scheduling round,
        // suspended that deque, and are now trying to find a new deque (hence the if let, since we
        // could have no active deque in this case). This means if we're ever stealing because
        // we've run out of jobs on our local active deque, our local active deque must be empty.
        // Thus, it serves no more use and needs to be freed/discarded/recycled. We also need to
        // remember to remove the corresponding stealer.
        // TODO: possible optimization where we don't free deques, but leave them allocated for
        // later use again (no idea if this would work).
        if let Some(active_deque) = unsafe { &mut *self.active_deque.get() }.take() {
            assert!(
                active_deque.is_empty(),
                "Trying to destroy active deque that was not empty"
            );

            // Destroy current active deque stealable resources
            // TODO: deque stealable resources must be destroyed before the actual deque worker is
            // destroyed, to avoid threads trying to steal from this deque when it's worker no
            // longer exists. How can we enforce this? Using memory barriers?
            self.stealables.destroy_deque(active_deque.id());

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
            .unwrap_or_else(|| {
                panic!(
                    "Could not find muggable deque {:?} in deque bench",
                    victim_deque_id
                )
            })
            .1;
        let popped_job = muggable_deque.pop();
        *unsafe { &mut *self.active_deque.get() } = Some(muggable_deque);

        popped_job
    }
}
