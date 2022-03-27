//! Debug Logging
//!
//! To use in a debug build, set the env var `RAYON_LOG` as
//! described below.  In a release build, logs are compiled out by
//! default unless Rayon is built with `--cfg rayon_rs_log` (try
//! `RUSTFLAGS="--cfg rayon_rs_log"`).
//!
//! Note that logs are an internally debugging tool and their format
//! is considered unstable, as are the details of how to enable them.
//!
//! # Valid values for RAYON_LOG
//!
//! The `RAYON_LOG` variable can take on the following values:
//!
//! * `tail:<file>` -- dumps the last 10,000 events into the given file;
//!   useful for tracking down deadlocks
//! * `profile:<file>` -- dumps only those events needed to reconstruct how
//!   many workers are active at a given time
//! * `all:<file>` -- dumps every event to the file; useful for debugging

use crate::deque::{DequeId, DequeState, ThreadIndex};
use crossbeam_channel::{self, Receiver, Sender};
use std::collections::VecDeque;
use std::env;
use std::fs::File;
use std::io::{self, BufWriter, Write};

/// True if logs are compiled in.
pub(super) const LOG_ENABLED: bool = cfg!(any(rayon_rs_log, debug_assertions));

#[derive(Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Debug)]
pub(super) enum Event {
    /// Flushes events to disk, used to terminate benchmarking.
    Flush,

    /// Indicates that a worker thread started execution.
    ThreadStart {
        worker: ThreadIndex,
        terminate_addr: usize,
    },

    /// Indicates that a worker thread terminated execution.
    ThreadTerminate {
        worker: ThreadIndex,
    },

    /// Indicates that a worker thread became idle, blocked on `latch_addr`.
    ThreadIdle {
        worker: ThreadIndex,
        latch_addr: usize,
    },

    /// Indicates that an idle worker thread found work to do, after
    /// yield rounds. It should no longer be considered idle.
    ThreadFoundWork {
        worker: ThreadIndex,
        yields: u32,
    },

    /// Idle worker thread did not find work
    ThreadDidNotFindWork {
        worker: ThreadIndex,
    },

    /// Indicates that a worker blocked on a latch observed that it was set.
    ///
    /// Internal debugging event that does not affect the state
    /// machine.
    ThreadSawLatchSet {
        worker: ThreadIndex,
        latch_addr: usize,
    },

    /// Indicates that an idle worker is getting sleepy. `sleepy_counter` is the internal
    /// sleep state that we saw at the time.
    ThreadSleepy {
        worker: ThreadIndex,
        jobs_counter: usize,
    },

    /// Indicates that the thread's attempt to fall asleep was
    /// interrupted because the latch was set. (This is not, in and of
    /// itself, a change to the thread state.)
    ThreadSleepInterruptedByLatch {
        worker: ThreadIndex,
        latch_addr: usize,
    },

    /// Indicates that the thread's attempt to fall asleep was
    /// interrupted because a job was posted. (This is not, in and of
    /// itself, a change to the thread state.)
    ThreadSleepInterruptedByJob {
        worker: ThreadIndex,
    },

    /// Indicates that an idle worker has gone to sleep.
    ThreadSleeping {
        worker: ThreadIndex,
        latch_addr: usize,
    },

    /// Indicates that a sleeping worker has awoken.
    ThreadAwoken {
        worker: ThreadIndex,
        latch_addr: usize,
    },

    /// Indicates that the given worker thread was notified it should
    /// awaken.
    ThreadNotify {
        worker: ThreadIndex,
    },

    /// Indicates that a waker was triggered
    /// suspended_deque_id: deque that was suspended and now resumable
    /// stealable_set_index: stealable set the deque was already in
    WakerAwoken {
        suspended_deque_id: DequeId,
        stealable_set_index: ThreadIndex,
    },

    /// Indicates that a waker was triggered, and the deque was added to a stealable set (as
    /// opposed to already being in one).
    /// suspended_deque_id: deque that was suspended and now resumable
    /// stealable_set_index: stealable set the deque was added to
    WakerAwokenAndDequeInserted {
        suspended_deque_id: DequeId,
        stealable_set_index: ThreadIndex,
    },

    /// Indicates that a FutureJob was executed and returned Poll::Ready.
    FutureJobReady {
        deque_id: DequeId,
        executing_worker_thread: ThreadIndex,
    },

    /// Indicates that a FutureJob was executed but returned Poll::Pending.
    FutureJobPending {
        deque_id: DequeId,
        executing_worker_thread: ThreadIndex,
    },

    /// The given worker has pushed a job to its local deque.
    JobPushed {
        worker: ThreadIndex,
        deque_id: DequeId,
    },

    /// The given worker has popped a job from its local deque.
    JobPopped {
        worker: ThreadIndex,
        deque_id: DequeId,
        jobs_remaining: usize,
    },

    /// The given worker has failed to pop a job from its local deque.
    JobPoppedFailed {
        worker: ThreadIndex,
        deque_id: DequeId,
        jobs_remaining: usize,
    },

    JobStealAttempt {
        attempt: usize,
        worker: ThreadIndex,
        victim_thread: ThreadIndex,
        victim_deque_id: DequeId,
        deque_state: DequeState,
    },

    /// The given worker has successfully stolen a job from the deque of another.
    JobStolen {
        attempt: usize,
        worker: ThreadIndex,
        victim_thread: ThreadIndex,
        victim_deque_id: DequeId,
        deque_state: DequeState,
    },

    /// The given worker attempted to steal a job, but failed because the stealer was empty
    JobStolenFailEmpty {
        attempt: usize,
        worker: ThreadIndex,
        victim_thread: ThreadIndex,
        victim_deque_id: DequeId,
        deque_state: DequeState,
    },

    /// The given worker attempted to steal a job, but failed because the stealer returned Retry
    JobStolenFailRetry {
        attempt: usize,
        worker: ThreadIndex,
        victim_thread: ThreadIndex,
        victim_deque_id: DequeId,
        deque_state: DequeState,
    },

    NewDequeAddedToStealableSet {
        stealable_set_index: ThreadIndex,
        deque_id: DequeId,
    },

    ExistingDequeAddedToStealableSet {
        stealable_set_index: ThreadIndex,
        deque_id: DequeId,
    },

    DequeRemovedFromStealableSet {
        stealable_set_index: ThreadIndex,
        deque_id: DequeId,
    },

    DequeRemovedFromStealableSetFailed {
        stealable_set_index: ThreadIndex,
        deque_id: DequeId,
    },

    DequeRemovedFromStealableSetNotPerfomed {
        deque_id: DequeId,
    },

    DequeStateUpdated {
        deque_id: DequeId,
        deque_state: DequeState,
    },

    RebalanceStealables {
        attempt: usize,
        thread_index: ThreadIndex,
        victim_index: ThreadIndex,
    },

    HandlingEmptyDeque {
        thread_doing_handling: ThreadIndex,
        stealable_set_index: Option<ThreadIndex>,
        deque_id: DequeId,
        deque_state: DequeState,
    },

    HandlingEmptyDequeNotEmpty {
        thread_doing_handling: ThreadIndex,
        stealable_set_index: Option<ThreadIndex>,
        deque_id: DequeId,
        deque_state: DequeState,
    },

    HandlingEmptyDequeNotFound {
        thread_doing_handling: ThreadIndex,
        deque_id: DequeId,
    },

    SettingToActive {
        worker: ThreadIndex,
        victim_thread: ThreadIndex,
        victim_deque_id: DequeId,
    },

    /// N jobs were injected into the global queue.
    JobsInjected {
        count: usize,
    },

    /// A job was removed from the global queue.
    JobUninjected {
        worker: ThreadIndex,
    },

    /// When announcing a job, this was the value of the counters we observed.
    ///
    /// No effect on thread state, just a debugging event.
    JobThreadCounts {
        worker: ThreadIndex,
        num_idle: u16,
        num_sleepers: u16,
    },
}

/// Handle to the logging thread, if any. You can use this to deliver
/// logs. You can also clone it freely.
#[derive(Clone)]
pub(super) struct Logger {
    sender: Option<Sender<Event>>,
}

impl Logger {
    pub(super) fn new(num_workers: usize) -> Logger {
        if !LOG_ENABLED {
            return Self::disabled();
        }

        // see the doc comment for the format
        let env_log = match env::var("RAYON_LOG") {
            Ok(s) => s,
            Err(_) => return Self::disabled(),
        };

        let (sender, receiver) = crossbeam_channel::unbounded();

        if env_log.starts_with("tail:") {
            let filename = env_log["tail:".len()..].to_string();
            ::std::thread::spawn(move || {
                Self::tail_logger_thread(num_workers, filename, 10_000, receiver)
            });
        } else if env_log == "all" {
            ::std::thread::spawn(move || Self::all_logger_thread(num_workers, receiver));
        } else if env_log.starts_with("profile:") {
            let filename = env_log["profile:".len()..].to_string();
            ::std::thread::spawn(move || {
                Self::profile_logger_thread(num_workers, filename, 10_000, receiver)
            });
        } else {
            panic!("RAYON_LOG should be 'tail:<file>' or 'profile:<file>'");
        }

        Logger {
            sender: Some(sender),
        }
    }

    fn disabled() -> Logger {
        Logger { sender: None }
    }

    #[inline]
    pub(super) fn log(&self, event: impl FnOnce() -> Event) {
        if !LOG_ENABLED {
            return;
        }

        if let Some(sender) = &self.sender {
            sender.send(event()).unwrap();
        }
    }

    fn profile_logger_thread(
        num_workers: usize,
        log_filename: String,
        capacity: usize,
        receiver: Receiver<Event>,
    ) {
        let file = File::create(&log_filename)
            .unwrap_or_else(|err| panic!("failed to open `{}`: {}", log_filename, err));

        let mut writer = BufWriter::new(file);
        let mut events = Vec::with_capacity(capacity);
        let mut state = SimulatorState::new(num_workers);
        let timeout = std::time::Duration::from_secs(30);

        loop {
            loop {
                match receiver.recv_timeout(timeout) {
                    Ok(event) => {
                        if let Event::Flush = event {
                            break;
                        } else {
                            events.push(event);
                        }
                    }

                    Err(_) => break,
                }

                if events.len() == capacity {
                    break;
                }
            }

            for event in events.drain(..) {
                if state.simulate(&event) {
                    state.dump(&mut writer, &event).unwrap();
                }
            }

            writer.flush().unwrap();
        }
    }

    fn tail_logger_thread(
        num_workers: usize,
        log_filename: String,
        capacity: usize,
        receiver: Receiver<Event>,
    ) {
        let file = File::create(&log_filename)
            .unwrap_or_else(|err| panic!("failed to open `{}`: {}", log_filename, err));

        let mut writer = BufWriter::new(file);
        let mut events: VecDeque<Event> = VecDeque::with_capacity(capacity);
        let mut state = SimulatorState::new(num_workers);
        let timeout = std::time::Duration::from_secs(30);
        let mut skipped = false;

        loop {
            loop {
                match receiver.recv_timeout(timeout) {
                    Ok(event) => {
                        if let Event::Flush = event {
                            // We ignore Flush events in tail mode --
                            // we're really just looking for
                            // deadlocks.
                            continue;
                        } else {
                            if events.len() == capacity {
                                let event = events.pop_front().unwrap();
                                state.simulate(&event);
                                skipped = true;
                            }

                            events.push_back(event);
                        }
                    }

                    Err(_) => break,
                }
            }

            if skipped {
                write!(writer, "...\n").unwrap();
                skipped = false;
            }

            for event in events.drain(..) {
                // In tail mode, we dump *all* events out, whether or
                // not they were 'interesting' to the state machine.
                state.simulate(&event);
                state.dump(&mut writer, &event).unwrap();
            }

            writer.flush().unwrap();
        }
    }

    fn all_logger_thread(num_workers: usize, receiver: Receiver<Event>) {
        let stderr = std::io::stderr();
        let mut state = SimulatorState::new(num_workers);

        for event in receiver {
            let mut writer = BufWriter::new(stderr.lock());
            state.simulate(&event);
            state.dump(&mut writer, &event).unwrap();
            writer.flush().unwrap();
        }
    }
}

#[derive(Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Debug)]
enum State {
    Working,
    Idle,
    Notified,
    Sleeping,
    Terminated,
}

impl State {
    fn letter(&self) -> char {
        match self {
            State::Working => 'W',
            State::Idle => 'I',
            State::Notified => 'N',
            State::Sleeping => 'S',
            State::Terminated => 'T',
        }
    }
}

struct SimulatorState {
    thread_states: Vec<State>,
    injector_size: usize,
}

impl SimulatorState {
    fn new(num_workers: usize) -> Self {
        Self {
            thread_states: vec![State::Working; num_workers],
            injector_size: 0,
        }
    }

    fn simulate(&mut self, event: &Event) -> bool {
        match *event {
            Event::ThreadIdle { worker, .. } => {
                assert_eq!(self.thread_states[worker], State::Working);
                self.thread_states[worker] = State::Idle;
                true
            }

            Event::ThreadStart { worker, .. } | Event::ThreadFoundWork { worker, .. } => {
                self.thread_states[worker] = State::Working;
                true
            }

            Event::ThreadTerminate { worker, .. } => {
                self.thread_states[worker] = State::Terminated;
                true
            }

            Event::ThreadSleeping { worker, .. } => {
                assert_eq!(self.thread_states[worker], State::Idle);
                self.thread_states[worker] = State::Sleeping;
                true
            }

            Event::ThreadAwoken { worker, .. } => {
                assert_eq!(self.thread_states[worker], State::Notified);
                self.thread_states[worker] = State::Idle;
                true
            }

            Event::JobsInjected { count } => {
                self.injector_size += count;
                true
            }

            Event::JobUninjected { .. } => {
                self.injector_size -= 1;
                true
            }

            Event::ThreadNotify { worker } => {
                // Currently, this log event occurs while holding the
                // thread lock, so we should *always* see it before
                // the worker awakens.
                assert_eq!(self.thread_states[worker], State::Sleeping);
                self.thread_states[worker] = State::Notified;
                true
            }

            // remaining events are no-ops from pov of simulating the
            // thread state
            _ => false,
        }
    }

    fn dump(&mut self, w: &mut impl Write, event: &Event) -> io::Result<()> {
        let num_idle_threads = self
            .thread_states
            .iter()
            .filter(|s| **s == State::Idle)
            .count();

        let num_sleeping_threads = self
            .thread_states
            .iter()
            .filter(|s| **s == State::Sleeping)
            .count();

        let num_notified_threads = self
            .thread_states
            .iter()
            .filter(|s| **s == State::Notified)
            .count();

        write!(w, "{:2},", num_idle_threads)?;
        write!(w, "{:2},", num_sleeping_threads)?;
        write!(w, "{:2},", num_notified_threads)?;
        write!(w, "{:4},", self.injector_size)?;

        let event_str = format!("{:?}", event);
        write!(w, r#""{:60}","#, event_str)?;

        write!(w, "\n")?;
        Ok(())
    }
}
