use crate::job::JobRef;
use crate::log::Event::*;
use crate::registry::{Registry, XorShift64Star};
use crossbeam_deque::Stealer;
use crossbeam_deque::Worker as CrossbeamWorker;
use dashmap::mapref::one::RefMut;
use dashmap::DashMap;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

thread_local! {
    static RNG: XorShift64Star = XorShift64Star::new();
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(super) enum DequeState {
    Active,
    Suspended,
    Resumable,
    Muggable,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub(super) struct DequeId(usize);

impl DequeId {
    #[must_use]
    #[inline]
    pub(super) fn new(id: usize) -> Self {
        Self(id)
    }
}

pub(super) struct Deque {
    deque: CrossbeamWorker<JobRef>,
    id: DequeId,
}

impl Deque {
    #[must_use]
    pub(super) fn new_fifo(id: DequeId) -> Self {
        Self {
            deque: CrossbeamWorker::new_fifo(),
            id,
        }
    }

    #[must_use]
    pub(super) fn new_lifo(id: DequeId) -> Self {
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
pub(super) type StealablesLock<'a> =
    RefMut<'a, DequeId, (Stealer<JobRef>, DequeState, Option<ThreadIndex>)>;
type Outcome = Result<(), ()>;

pub(super) struct Stealables {
    stealable_sets: Vec<StealableSet>,
    // ThreadIndex represents the thread
    // index that has this deque in its
    // stealable set (a deque can be unstealable
    // and not belong in a stealable set so
    // it has no ThreadIndex: when it is suspended
    // and has no ready jobs; we still keep the
    // stealer here in the mapping for
    // implementation simplicity)
    deque_stealers: DashMap<DequeId, (Stealer<JobRef>, DequeState, Option<ThreadIndex>)>,

    registry: Option<Arc<Registry>>, // just used for logging
}

impl Stealables {
    pub(super) fn set_registry(&mut self, registry: Arc<Registry>) {
        self.registry = Some(registry);
    }

    #[must_use]
    #[inline]
    pub(super) fn get_lock(&self, deque_id: DequeId) -> Option<StealablesLock<'_>> {
        self.deque_stealers.get_mut(&deque_id)
    }

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
    pub(super) fn update_deque_state(
        &self,
        lock: Option<&mut StealablesLock<'_>>,
        deque_id: DequeId,
        deque_state: DequeState,
    ) {
        if let Some(lock) = lock {
            lock.value_mut().1 = deque_state;
        } else {
            self.deque_stealers
                .get_mut(&deque_id)
                .expect("Deque ID should have been in stealables mapping, but was not found")
                .value_mut()
                .1 = deque_state;
        }
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
    #[inline]
    pub(super) fn get_random_deque_id(&self, thread_index: ThreadIndex) -> Option<DequeId> {
        self.stealable_sets[thread_index].get_random_deque_id()
    }

    // TODO: this function doesn't really belong to Stealables, try and find a nicer way to get the
    // number of worker threads
    #[must_use]
    #[inline]
    pub(super) fn get_num_threads(&self) -> usize {
        self.stealable_sets.len()
    }

    pub(super) fn add_new_deque_to_stealable_set(
        &self,
        thread_index: ThreadIndex,
        deque: &Deque,
        deque_state: DequeState,
    ) {
        assert!(thread_index < self.stealable_sets.len());

        let key_already_existed = self
            .deque_stealers
            .insert(deque.id, (deque.stealer(), deque_state, Some(thread_index)));

        assert!(
            key_already_existed.is_none(),
            "Stealable set already contained deque ID, when trying to add new deque"
        );

        self.registry
            .as_ref()
            .unwrap()
            .log(|| NewDequeAddedToStealableSet {
                stealable_set_index: thread_index,
                deque_id: deque.id,
            });
    }

    /// Use this if the deque already exists in the deque_stealers mapping, otherwise use
    /// [`Stealables::add_new_deque_to_stealable_set`]
    pub(super) fn add_existing_deque_to_stealable_set(
        &self,
        lock: Option<&mut StealablesLock<'_>>,
        thread_index: ThreadIndex,
        deque_id: DequeId,
    ) {
        assert!(thread_index < self.stealable_sets.len());

        let mut s;
        let stealable_set_index = if let Some(lock) = lock {
            let (_, _, stealable_set_index) = lock.value_mut();
            stealable_set_index
        } else {
            // Get a reference here to ensure we hold internal DashMap lock for entire scope of function
            s = self
                .deque_stealers
                .get_mut(&deque_id)
                .expect("Deque ID should have been in stealables mapping, but was not found");
            let (_, _, stealable_set_index) = s.value_mut();
            stealable_set_index
        };

        match stealable_set_index {
            // Deque recorded to already be in stealable set we want to move deque into, no need to
            // do anything
            Some(index) if *index == thread_index => {}
            Some(_) | None => {
                // Make sure to keep track of which threads stealable set this deque is in
                *stealable_set_index = Some(thread_index);

                // Add deque to selected threads stealable set
                self.stealable_sets[thread_index].add_deque(deque_id);

                self.registry
                    .as_ref()
                    .unwrap()
                    .log(|| ExistingDequeAddedToStealableSet {
                        stealable_set_index: thread_index,
                        deque_id,
                    });
            }
        }
    }

    /// Removes a deque from its stealable set. Note that if you want to *destroy* a deque (i.e.
    /// free the deque and clean up all resources associated with it), call
    /// [`Stealables::destroy_deque`] instead.
    pub(super) fn remove_deque_from_stealable_set(
        &self,
        lock: Option<&mut StealablesLock<'_>>,
        deque_id: DequeId,
    ) -> Result<ThreadIndex, ()> {
        let mut s;
        // TODO: should probably refactor this and other methods to use get_lock
        let stealable_set_index = if let Some(lock) = lock {
            let (_, _, stealable_set_index) = lock.value_mut();
            stealable_set_index
        } else {
            // Get a reference here to ensure we hold internal DashMap lock for scope entire of function
            s = self.deque_stealers.get_mut(&deque_id).expect(
                "Deque stealer already destroyed when trying to remove deque from stealable set",
            );
            let (_, _, stealable_set_index) = s.value_mut();
            stealable_set_index
        };

        // Only try to remove a deque from a stealable set if it is recorded as not being in a
        // stealable set. Otherwise, since it is (apparently) not in a stealable set there is
        // nothing to be done.
        if let Some(set_index) = *stealable_set_index {
            // Make sure to keep track that this deque isn't found in any stealable set
            *stealable_set_index = None;

            // Remove deque from selected threads stealable set.
            // It's possible (I think) that another thread has removed this deque from a stealable
            // set before this thread has had the chance to remove it.
            match self.stealable_sets[set_index].remove_deque(deque_id) {
                Ok(_) => {
                    self.registry
                        .as_ref()
                        .unwrap()
                        .log(|| DequeRemovedFromStealableSet {
                            stealable_set_index: set_index,
                            deque_id,
                        });

                    Ok(set_index)
                }
                // TODO: I think this should actually never occur, since we are holding a DashMap lock
                // above, and really we should panic in this case
                Err(_) => {
                    self.registry
                        .as_ref()
                        .unwrap()
                        .log(|| DequeRemovedFromStealableSetFailed {
                            stealable_set_index: set_index,
                            deque_id,
                        });

                    Err(())
                }
            }
        } else {
            self.registry
                .as_ref()
                .unwrap()
                .log(|| DequeRemovedFromStealableSetNotPerfomed { deque_id });

            Err(())
        }
    }

    /// This method destroys the corresponding stealer and state information for a deque. This
    /// should only be called when a deque is being destroyed (i.e. freed). Basically, we no longer
    /// are going to use this deque, so we need to clean up our stealing resources. If you only
    /// want to remove a deque from a thread's *stealable set* instead without destroying the
    /// deque, use [`Stealables::remove_deque_from_stealable_set`].
    pub(super) fn destroy_deque(&self, deque_id: DequeId) {
        // First, remove deque from the stealable set it is currently in
        let _ = self.remove_deque_from_stealable_set(None, deque_id);

        // Next, remove all data associated with this deque, by letting things go out of scope
        // TODO: how to enforce order that deque must be removed from stealable set before having
        // its resources destroyed? Another thread must not try to steal a deque that is being
        // destroyed. Memory barriers?
        let _ = self
            .deque_stealers
            .remove(&deque_id)
            .expect("Deque stealer to be destroyed was not found");
    }

    /// Move a stealable deque from another random victim thread stealable set into this thread's
    /// stealable set.
    pub(super) fn rebalance_stealables<'a>(
        &'a self,
        mut lock: Option<&mut StealablesLock<'a>>,
        thread_index: ThreadIndex,
    ) {
        let mut rebalance_closure = |attempt| -> Outcome {
            let victim_index = RNG.with(|rng| rng.next_usize(self.get_num_threads()));

            self.registry.as_ref().unwrap().log(|| RebalanceStealables {
                attempt,
                thread_index,
                victim_index,
            });

            // No need to do anything if victim thread is same as this thread
            if thread_index == victim_index {
                return Ok(());
            }

            if let Some(stealable_deque_id) = self.get_random_deque_id(victim_index) {
                // We need a lock, since we want the rebalacing operation to be atomic (we don't
                // want there to be a split moment where this deque isn't in a stealable set in
                // between the removal and adding operations, since that doesn't reflect the true
                // state of the deque/stealable sets)
                let lock = &mut lock;

                if let Some(lock) = lock {
                    let deque_state = lock.value().1;

                    if deque_state != DequeState::Active {
                        // Move deque from victim thread stealable set to this thread stealable set
                        self.remove_deque_from_stealable_set(Some(*lock), stealable_deque_id)?;
                        self.add_existing_deque_to_stealable_set(
                            Some(*lock),
                            thread_index,
                            stealable_deque_id,
                        );
                    } else {
                        // We need to make sure rebalancing doesn't move worker active deques
                        return Err(());
                    }
                } else if let Some(mut lock) = self.get_lock(stealable_deque_id) {
                    let deque_state = lock.value().1;

                    if deque_state != DequeState::Active {
                        // Move deque from victim thread stealable set to this thread stealable set
                        self.remove_deque_from_stealable_set(Some(&mut lock), stealable_deque_id)?;
                        self.add_existing_deque_to_stealable_set(
                            Some(&mut lock),
                            thread_index,
                            stealable_deque_id,
                        );
                    } else {
                        // We need to make sure rebalancing doesn't move worker active deques
                        return Err(());
                    }
                } else {
                    return Err(());
                }

                Ok(())
            } else {
                Err(())
            }
        };

        let rebalance_attempts = 5; // TODO: totally arbitrary, find a better cap

        for i in 0..rebalance_attempts {
            if let Ok(_) = rebalance_closure(i) {
                return;
            }
        }
    }

    /// This method takes care of a deque in a stealable set that is empty (has no jobs to
    /// execute). This is done here, so that we can take advantage of the internal DashMap lock.
    /// Returns true if deque is empty.
    pub(super) fn handle_empty_deque(&self, deque_id: DequeId) -> bool {
        let mut lock = self
            .get_lock(deque_id)
            .expect("Deque ID should have been in stealables mapping, but was not found");
        let (deque_stealer, deque_state, _) = lock.value_mut();
        let deque_state = *deque_state;

        if deque_stealer.is_empty() {
            self.registry.as_ref().unwrap().log(|| HandlingEmptyDeque {
                deque_id,
                deque_state,
            });

            // This deque is empty and has no jobs to be stolen from, so remove it from
            // its stealable set
            let index = self.remove_deque_from_stealable_set(Some(&mut lock), deque_id);

            // If the deque is not Suspended (Suspended deque can be empty but awaiting
            // suspended future to awake) but is empty, we should destroy/free the
            // deque since no thread can steal from this deque, and if it is an empty
            // Active deque a stealing thread will allocate a fresh deque anyway
            if deque_state != DequeState::Suspended {
                // TODO: free deque
                // TODO: potential deque recycling optimization
            }

            // Only rebalance if this thread was indeed responsible for removal of deque from
            // stealable set. This is because another thread could have removed this deque from its
            // stealable set before this thread had a chance to, and if so there is no need for
            // this thread to perform rebalancing.
            if let Ok(index) = index {
                self.rebalance_stealables(Some(&mut lock), index);
            }

            true
        } else {
            self.registry
                .as_ref()
                .unwrap()
                .log(|| HandlingEmptyDequeNotEmpty {
                    deque_id,
                    deque_state,
                });

            false
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
            stealable_sets.last_mut().unwrap().index = index;
        }

        Self {
            stealable_sets,
            deque_stealers,
            registry: None,
        }
    }
}

/// Basically contains a set of stealable deque IDs
struct StealableSet {
    stealable_deque_ids: Mutex<(HashMap<DequeId, usize>, Vec<DequeId>, XorShift64Star)>, // TODO: maybe use RwLock?
    index: ThreadIndex, // TODO: remove this, just using temporarily for debugging
}

impl StealableSet {
    /// Returns a random deque ID in this stealable set, otherwise None if it contains no deque
    /// ID's
    #[must_use]
    fn get_random_deque_id(&self) -> Option<DequeId> {
        let (_, v, rng) = &*self.stealable_deque_ids.lock().unwrap();

        // eprintln!(
        //     "stealable_set_index: {} getting random deque: {:?}",
        //     self.index, v
        // );

        if v.is_empty() {
            return None;
        }

        let random_index = rng.next_usize(v.len());
        Some(v[random_index])
    }

    fn add_deque(&self, deque_id: DequeId) {
        let (map, v, _) = &mut *self.stealable_deque_ids.lock().unwrap();

        assert!(
            !map.contains_key(&deque_id),
            "Deque already exists in stealable set, when trying to add deque"
        );

        v.push(deque_id);
        map.insert(deque_id, v.len() - 1);
    }

    fn remove_deque(&self, deque_id: DequeId) -> Outcome {
        let (map, v, _) = &mut *self.stealable_deque_ids.lock().unwrap();

        // Bail early if deque no longer in stealable set. This can happen if another thread beats
        // this thread in removing it, e.g. when rebalancing or stealing.
        if let Some(index) = map.remove(&deque_id) {
            let popped_deque_id = v.pop().expect(
                "When removing deque from this stealable set, stealable set erroneously empty somehow",
            );

            // only if deque we want to remove is *not* in the tail position of the vector do we need
            // to actually perform the tail swap maneuver
            if index < v.len() {
                v[index] = popped_deque_id;
                *map.get_mut(&popped_deque_id)
                    .expect("Stealable set mapping somehow nonexistent") = index;
            }

            Ok(())
        } else {
            Err(())
            // unreachable!(); // TODO: see if the DashMap lock in remove_deque_from_stealable_set is enough to prevent this case from happening here
        }
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
            index: 0,
        }
    }
}
