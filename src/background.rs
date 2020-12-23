use async_trait::async_trait;
use crossbeam_channel::{Receiver, RecvTimeoutError, TryRecvError};
use std::future::Future;
use std::io::Read;
use std::panic::{catch_unwind, UnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};
use std::thread;
/// Tiny thread-backed background job thing
///
/// This is very similar to ffi_helper's Task
/// https://github.com/Michael-F-Bryan/ffi_helpers
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ControlToken<S>(Arc<ControlTokenInner<S>>);

#[derive(Debug, Default)]
pub struct ControlTokenInner<S> {
    cancel: AtomicBool,
    pause: AtomicBool,
    status: Mutex<Option<S>>,
}

impl<S> ControlToken<S> {
    pub fn new() -> Self {
        Self(Arc::new(ControlTokenInner {
            cancel: AtomicBool::new(false),
            pause: AtomicBool::new(false),
            status: Mutex::new(None),
        }))
    }

    pub fn cancel(&self) {
        self.0.cancel.store(true, Ordering::SeqCst);
    }

    pub fn pause(&self) {
        self.0.pause.store(true, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.0.pause.store(false, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.cancel.load(Ordering::SeqCst)
    }

    pub fn is_paused(&self) -> bool {
        self.0.pause.load(Ordering::SeqCst)
    }

    pub fn is_cancelled_with_pause(&self) -> bool {
        self.is_cancelled() || (self.handle_pause() && self.is_cancelled())
    }

    pub fn handle_pause(&self) -> bool {
        let mut paused = false;

        while self.is_paused() && !self.is_cancelled() {
            paused = true;
            thread::park();
        }

        paused
    }

    pub fn set_status(&self, status: S) {
        let mut previous = self.0.status.lock().expect("status lock");
        previous.replace(status);
    }

    pub fn get_status(&self) -> Option<S> {
        let mut current = self.0.status.lock().expect("status lock");
        current.take()
    }

    pub fn result(&self) -> Result<(), ()> {
        if self.is_cancelled() {
            Err(())
        } else {
            Ok(())
        }
    }
}

impl<S> Default for ControlToken<S> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BackgroundHandle<T, S> {
    result: Receiver<std::thread::Result<T>>,
    control: ControlToken<S>,
    thread: thread::Thread,
}

impl<T, S> BackgroundHandle<T, S> {
    pub fn spawn<K>(task: K) -> BackgroundHandle<T, S>
    where
        K: Background<Output = T, Status = S> + UnwindSafe + Send + Sync + 'static,
        T: Send + Sync + 'static,
        S: Send + Sync + Clone + 'static,
    {
        let (tx, rx) = crossbeam_channel::unbounded();
        let control = ControlToken::new();
        let inner_control = control.clone();

        let handle = thread::spawn(move || {
            let response = catch_unwind(move || task.run(&inner_control));
            let _ = tx.send(response);
        });

        let thread = handle.thread().clone();

        BackgroundHandle {
            result: rx,
            control,
            thread,
        }
    }

    pub fn poll(&self) -> Option<T> {
        match self.result.try_recv() {
            Ok(value) => Some(value.unwrap()),
            Err(TryRecvError::Empty) => None,
            Err(e) => panic!("{:?}", e),
        }
    }

    pub fn wait_timeout(&self, wait: Duration) -> Option<T> {
        match self.result.recv_timeout(wait) {
            Ok(value) => Some(value.unwrap()),
            Err(RecvTimeoutError::Timeout) => None,
            Err(e) => panic!("{:?}", e),
        }
    }

    pub fn wait(self) -> T {
        match self.result.recv() {
            Ok(value) => value.unwrap(),
            Err(e) => panic!("{:?}", e),
        }
    }

    pub fn cancel(&self) {
        self.control.cancel();
        self.thread.unpark();
    }

    pub fn is_cancelled(&self) -> bool {
        self.control.is_cancelled()
    }

    pub fn status(&self) -> Option<S> {
        self.control.get_status()
    }

    pub fn pause(&self) {
        self.control.pause();
    }

    pub fn resume(&self) {
        self.control.resume();
        self.thread.unpark();
    }

    pub fn is_paused(&self) -> bool {
        self.control.is_paused()
    }
}

impl<T, S> Drop for BackgroundHandle<T, S> {
    fn drop(&mut self) {
        self.cancel();
    }
}

pub trait Background: Send + Sync {
    type Output: Send + Sync;
    type Status: Send + Sync;

    fn run(self, control: &ControlToken<Self::Status>) -> Self::Output;
}

pub struct FutureControlToken<S> {
    status: Option<S>,
}

#[async_trait]
pub trait BackgroundFuture {
    type Input: Send;

    async fn run(&self, input: Self::Input);
}

#[async_trait]
impl<Inner: Send + Sync> BackgroundFuture for Arc<Inner>
where
    Inner: BackgroundFuture,
{
    type Input = Inner::Input;

    async fn run(&self, input: Self::Input) {
        (&**self).run(input).await
    }
}

#[async_trait]
impl<Inner: Send + Sync> BackgroundFuture for &Inner
where
    Inner: BackgroundFuture,
{
    type Input = Inner::Input;

    async fn run(&self, input: Self::Input) {
        (&**self).run(input).await
    }
}

#[must_use = "yield_now does nothing unless polled/`await`-ed"]
pub fn yield_now() -> impl Future<Output = ()> {
    /// Yield implementation
    struct YieldNow {
        yielded: bool,
    }

    impl Future for YieldNow {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.yielded {
                return Poll::Ready(());
            }

            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    YieldNow { yielded: false }
}

struct PauseState {
    paused: bool,
    wakers: Vec<Waker>,
}

#[derive(Clone)]
pub struct Pause(Arc<Mutex<PauseState>>);

impl Pause {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(PauseState {
            paused: false,
            wakers: Vec::new(),
        })))
    }

    pub fn wrap<F: Send + Sync>(&self, f: F) -> PausableBackground<F>
    where
        F: BackgroundFuture,
    {
        PausableBackground {
            pause: self.clone(),
            inner: f,
        }
    }

    pub fn wrap_future<F: Future + Unpin>(&self, f: F) -> impl Future<Output = F::Output> {
        PauseFuture {
            inner: f,
            pause: self.clone(),
        }
    }
}

impl Pause {
    pub fn pause(&self) {
        let mut pause = self.0.lock().expect("pause lock");
        pause.paused = true;
    }

    pub fn resume(&self) {
        let mut pause = self.0.lock().expect("pause lock");
        pause.paused = false;
        for waker in pause.wakers.drain(..) {
            waker.wake();
        }
    }

    fn handle_pause(&self, waker: &Waker) -> bool {
        let mut pause = self.0.lock().expect("pause lock");
        if pause.paused {
            pause.wakers.push(waker.clone());
        }
        pause.paused
    }
}

struct PauseFuture<F> {
    inner: F,
    pause: Pause,
}

impl<F: Future + Unpin> Future for PauseFuture<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Scope to drop mutex before polling inner
        if self.pause.handle_pause(cx.waker()) {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll(cx)
    }
}

pub struct MultithreadedBackground<Inner> {
    handle: tokio::runtime::Handle,
    inner: Arc<Inner>,
}

impl<Inner: BackgroundFuture + Send + Sync + 'static> MultithreadedBackground<Inner> {
    pub fn new(handle: tokio::runtime::Handle, inner: Inner) -> Self {
        Self {
            handle,
            inner: Arc::new(inner),
        }
    }
}

#[async_trait]
impl<Inner: Send + Sync + 'static> BackgroundFuture for MultithreadedBackground<Inner>
where
    Inner: BackgroundFuture,
{
    type Input = Inner::Input;

    async fn run(&self, input: Self::Input) {
        let inner = Arc::clone(&self.inner);
        let _ = self
            .handle
            .spawn(async move {
                inner.run(input).await;
            })
            .await;
    }
}

pub struct PausableBackground<Inner> {
    pause: Pause,
    inner: Inner,
}

#[async_trait]
impl<Inner: Send + Sync + 'static> BackgroundFuture for PausableBackground<Inner>
where
    Inner: BackgroundFuture,
{
    type Input = Inner::Input;

    async fn run(&self, input: Self::Input) {
        self.pause.wrap_future(self.inner.run(input)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[derive(Debug, Clone, Copy)]
    pub struct Tick;

    impl Background for Tick {
        type Output = Result<u32, u32>;
        type Status = u32;

        fn run(self, control: &ControlToken<Self::Status>) -> Self::Output {
            let mut ticks = 0;

            while ticks < 100 && !control.is_cancelled_with_pause() {
                ticks += 1;
                control.set_status(ticks);

                thread::sleep(Duration::from_millis(10));
            }

            control.result().map(|_| ticks).map_err(|_| ticks)
        }
    }

    #[test]
    fn it_cancels() {
        let task = Tick;

        let handle = BackgroundHandle::spawn(task);

        for _ in 0..10 {
            thread::sleep(Duration::from_millis(10));
            let got = handle.poll();
            assert!(got.is_none());
        }

        handle.cancel();

        let ret = handle.wait();
        assert!(ret.is_err());
        let ticks = ret.unwrap_err();
        assert!(9 <= ticks && ticks <= 12);
    }

    #[test]
    fn it_pauses() {
        let task = Tick;

        let handle = BackgroundHandle::spawn(task);

        handle.pause();

        for _ in 0..10 {
            thread::sleep(Duration::from_millis(10));
            let got = handle.poll();
            assert!(got.is_none());
        }

        handle.resume();

        for _ in 0..10 {
            thread::sleep(Duration::from_millis(10));
            let got = handle.poll();
            assert!(got.is_none());
        }

        handle.cancel();

        let ret = handle.wait();
        assert!(ret.is_err());
        let ticks = ret.unwrap_err();
        assert!(9 <= ticks && ticks <= 12);
    }
}
