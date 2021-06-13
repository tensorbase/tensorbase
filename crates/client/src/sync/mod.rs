// TODO:  draw up condvar such a https://docs.rs/async-std/1.6.2/async_std/sync/struct.Condvar.html
// Condvar is better in task synchronization than tokio notifier in our case.
// While stopping pool we have to signal all pending tasks that it is not possible for notifier.
//
mod condvar {}
mod waker_set;
pub use waker_set::WakerSet;
