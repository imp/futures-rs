use std::cmp::{Eq, PartialEq, PartialOrd, Ord, Ordering};
use std::collections::BinaryHeap;
use std::fmt::{self, Debug};
use std::iter::FromIterator;

use futures_core::{Async, Future, IntoFuture, Poll, Stream};
use futures_core::task;

use stream::FuturesUnordered;

#[must_use = "futures do nothing unless polled"]
#[derive(Debug)]
struct OrderWrapper<T, Idx> {
    item: T,
    index: Idx,
}

impl<T, Idx> PartialEq for OrderWrapper<T, Idx>
where
    Idx: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl<T, Idx> Eq for OrderWrapper<T, Idx> {}

impl<T, Idx> PartialOrd for OrderWrapper<T, Idx>
where
    Idx: PartialOrd + Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T, Idx> Ord for OrderWrapper<T, Idx>
where
    Idx: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max heap, so compare backwards here.
        other.index.cmp(&self.index)
    }
}

impl<T, Idx> Future for OrderWrapper<T, Idx>
where
    T: Future,
{
    type Item = OrderWrapper<T::Item, Idx>;
    type Error = T::Error;

    fn poll(&mut self, cx: &mut task::Context) -> Poll<Self::Item, Self::Error> {
        let result = try_ready!(self.item.poll(cx));
        Ok(Async::Ready(OrderWrapper {
            item: result,
            index: self.index
        }))
    }
}

/// An unbounded queue of futures.
///
/// This "combinator" is similar to `FuturesUnordered`, but it imposes an order
/// on top of the set of futures. While futures in the set will race to
/// completion in parallel, results will only be returned in the order specified
/// when their originating futures were added to the queue.
///
/// Futures and their ordering index are pushed into this queue and their realized
/// values are yielded in order. This structure is optimized to manage a large
/// number of futures. Futures managed by `FuturesIndexed` will only be polled when
/// they generate notifications. This reduces the required amount of work needed
/// to coordinate large numbers of futures.
///
/// When a `FuturesIndexed` is first created, it does not contain any futures.
/// Calling `poll` in this state will result in `Ok(Async::Ready(None))` to be
/// returned. Futures are submitted to the queue using `push`; however, the
/// future will **not** be polled at this point. `FuturesIndexed` will only
/// poll managed futures when `FuturesIndexed::poll` is called. As such, it
/// is important to call `poll` after pushing new futures.
///
/// If `FuturesIndexed::poll` returns `Ok(Async::Ready(None))` this means that
/// the queue is currently not managing any futures. A future may be submitted
/// to the queue at a later time. At that point, a call to
/// `FuturesIndexed::poll` will either return the future's resolved value
/// **or** `Ok(Async::Pending)` if the future has not yet completed. When
/// multiple futures are submitted to the queue, `FuturesIndexed::poll` will
/// return `Ok(Async::Pending)` until the first future completes, even if
/// some of the later futures have already completed.
///
/// Note that you can create a ready-made `FuturesIndexed` via the
/// `futures_ordered` function in the `stream` module, or you can start with an
/// empty queue with the `FuturesIndexed::new` constructor.
#[must_use = "streams do nothing unless polled"]
pub struct FuturesIndexed<T, Idx>
where
    T: Future,
{
    in_progress: FuturesUnordered<OrderWrapper<T, Idx>>,
    queued_results: BinaryHeap<OrderWrapper<T::Item, Idx>>,
    // next_incoming_index: usize,
    next_outgoing_index: Idx,
}

/// Converts a list of futures into a `Stream` of results from the futures.
///
/// This function will take an list of futures (e.g. a vector, an iterator,
/// etc), and return a stream. The stream will yield items as they become
/// available on the futures internally, in the order that their originating
/// futures were submitted to the queue. If the futures complete out of order,
/// items will be stored internally within `FuturesIndexed` until all preceding
/// items have been yielded.
///
/// Note that the returned queue can also be used to dynamically push more
/// futures into the queue as they become available.
pub fn futures_ordered<I>(futures: I) -> FuturesIndexed<<I::Item as IntoFuture>::Future, usize>
where
    I: IntoIterator,
    I::Item: IntoFuture,
{
    let mut queue = FuturesIndexed::new();

    for (index, future) in futures.into_iter().enumerate() {
        queue.push(index, future.into_future());
    }

    queue
}

impl<T, Idx> FuturesIndexed<T, Idx>
where
    T: Future,
    Idx: Ord + Default,
{
    /// Constructs a new, empty `FuturesIndexed`
    ///
    /// The returned `FuturesIndexed` does not contain any futures and, in this
    /// state, `FuturesIndexed::poll` will return `Ok(Async::Ready(None))`.
    pub fn new() -> FuturesIndexed<T, Idx> {
        FuturesIndexed {
            in_progress: FuturesUnordered::new(),
            queued_results: BinaryHeap::new(),
            // next_incoming_index: 0,
            next_outgoing_index: Idx::default(),
        }
    }

    /// Returns the number of futures contained in the queue.
    ///
    /// This represents the total number of in-flight futures, both
    /// those currently processing and those that have completed but
    /// which are waiting for earlier futures to complete.
    pub fn len(&self) -> usize {
        self.in_progress.len() + self.queued_results.len()
    }

    /// Returns `true` if the queue contains no futures
    pub fn is_empty(&self) -> bool {
        self.in_progress.is_empty() && self.queued_results.is_empty()
    }

    /// Push a future into the queue.
    ///
    /// This function submits the given future to the internal set for managing.
    /// This function will not call `poll` on the submitted future. The caller
    /// must ensure that `FuturesIndexed::poll` is called in order to receive
    /// task notifications.
    pub fn push(&mut self, index: Idx, future: T) {
        let wrapped = OrderWrapper {
            item: future,
            index,
        };
        self.in_progress.push(wrapped);
    }
}

impl<T, Idx> Stream for FuturesIndexed<T, Idx>
where
    T: Future,
    Idx: Ord + AddAssign,
{
    type Item = T::Item;
    type Error = T::Error;

    fn poll(&mut self, cx: &mut task::Context) -> Poll<Option<Self::Item>, Self::Error> {
        // Get any completed futures from the unordered set.
        loop {
            match self.in_progress.poll(cx)? {
                Async::Ready(Some(result)) => self.queued_results.push(result),
                Async::Ready(None) | Async::Pending => break,
            }
        }

        if let Some(next_result) = self.queued_results.peek() {
            // PeekMut::pop is not stable yet QQ
            if next_result.index != self.next_outgoing_index {
                return Ok(Async::Pending);
            }
        } else if !self.in_progress.is_empty() {
            return Ok(Async::Pending);
        } else {
            return Ok(Async::Ready(None));
        }

        let next_result = self.queued_results.pop().unwrap();
        self.next_outgoing_index += 1;
        Ok(Async::Ready(Some(next_result.item)))
    }
}

impl<T, Idx> Debug for FuturesIndexed<T, Idx>
where
    T: Future + Debug,
    Idx: Debug,

{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "FuturesIndexed {{ ... }}")
    }
}

impl<F: Future> FromIterator<F> for FuturesIndexed<F> {
    fn from_iter<T>(iter: T) -> Self
        where T: IntoIterator<Item = F>
    {
        let mut new = FuturesIndexed::new();
        for future in iter.into_iter() {
            new.push(future);
        }
        new
    }
}
