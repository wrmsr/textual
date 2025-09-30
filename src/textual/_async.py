import asyncio
import sys
import typing as ta


T = ta.TypeVar('T')
T_co = ta.TypeVar('T_co', covariant=True)


CancelledError = asyncio.CancelledError
QueueEmpty = asyncio.QueueEmpty
TimeoutError = asyncio.TimeoutError  # noqa


##


def sleep(delay):
    return asyncio.sleep(delay)


##


class Future(ta.Protocol[T]):
    def cancel(self) -> bool: ...
    def add_done_callback(self, fn) -> None: ...
    def set_result(self, result: T) -> None: ...
    def cancelled(self) -> bool: ...
    def done(self) -> bool: ...
    def result(self) -> T: ...
    def exception(self) -> BaseException | None: ...

    def __await__(self) -> ta.Generator[ta.Any, None, T]: ...


def new_future() -> Future:
    return asyncio.Future()


##


class Task(Future[T], ta.Protocol[T]):
    pass


def current_task() -> Task:
    return asyncio.current_task()


def create_task(coro, name=None, context=None) -> Task:
    return asyncio.create_task(coro, name=name, context=context)


##


class TimerHandle(ta.Protocol):
    def cancel(self) -> None: ...


def call_later(
        delay: float,
        callback: ta.Callable
) -> TimerHandle:
    return asyncio.get_running_loop().call_later(delay, callback)


##


async def run_in_executor(fn, *args):
    return await asyncio.get_running_loop().run_in_executor(None, fn, *args)


##


def gather(*coros_or_futures, return_exceptions=False):
    return asyncio.gather(*coros_or_futures, return_exceptions=return_exceptions)


ALL_COMPLETED = asyncio.ALL_COMPLETED
FIRST_COMPLETED = asyncio.FIRST_COMPLETED
FIRST_EXCEPTION = asyncio.FIRST_EXCEPTION


def wait(fs, *, timeout=None, return_when=ALL_COMPLETED):
    return asyncio.wait(fs, timeout=timeout, return_when=return_when)


def wait_for(fut, timeout):
    return asyncio.wait_for(fut, timeout=timeout)


##


class Loop(ta.Protocol):
    def add_signal_handler(self, sig, callback, *args): ...
    def call_soon_threadsafe(self, callback, *args, context=None): ...
    def create_future(self) -> Future: ...
    def run_in_executor(self, executor, func, *args): ...
    def run_until_complete(self, future): ...


def get_running_loop() -> Loop:
    return asyncio.get_running_loop()


def run_coroutine_threadsafe(coro, loop):
    return asyncio.run_coroutine_threadsafe(coro, loop)


def set_loop_eager_task_factory(loop):
    if hasattr(asyncio, "eager_task_factory"):
        loop.set_task_factory(asyncio.eager_task_factory)


##


def shield(arg):
    return asyncio.shield(arg)


##


def run(main):
    return asyncio.run(main)


_ASYNCIO_GET_EVENT_LOOP_IS_DEPRECATED = sys.version_info >= (3, 10, 0)


def run_main(fn):
    if _ASYNCIO_GET_EVENT_LOOP_IS_DEPRECATED:
        # N.B. This does work with Python<3.10, but global Locks, Events, etc
        # eagerly bind the event loop, and result in Future bound to wrong
        # loop errors.
        return run(fn())
    try:
        global_loop = asyncio.get_event_loop()
    except RuntimeError:
        # the global event loop may have been destroyed by someone running
        # asyncio.run(), or asyncio.set_event_loop(None), in which case
        # we need to use asyncio.run() also. (We run this outside the
        # context of an exception handler)
        pass
    else:
        return global_loop.run_until_complete(fn())
    return run(fn())


##


class Event(ta.Protocol):
    def is_set(self): ...
    def set(self): ...
    def clear(self): ...
    async def wait(self): ...


def new_event() -> Event:
    return asyncio.Event()


##


class Lock(ta.Protocol):
    def locked(self): ...
    async def acquire(self): ...
    def release(self): ...


def new_lock() -> Lock:
    return asyncio.Lock()


##


class Queue(ta.Protocol[T_co]):
    async def get(self): ...
    def task_done(self): ...
    def put_nowait(self, item): ...
    async def join(self): ...
    def empty(self): ...
    async def put(self, item): ...


def new_queue() -> Queue:
    return asyncio.Queue()

