from __future__ import annotations

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


class Future(ta.Generic[T]):
    def __init__(self, *, _underlying: asyncio.Future[T]) -> None:
        super().__init__()
        self._underlying = _underlying

    def cancel(self) -> bool:
        return self._underlying.cancel()

    def add_done_callback(self, fn) -> None:
        self._underlying.add_done_callback(fn)

    def set_result(self, result: T) -> None:
        self._underlying.set_result(result)

    def cancelled(self) -> bool:
        return self._underlying.cancelled()

    def done(self) -> bool:
        return self._underlying.done()

    def result(self) -> T:
        return self._underlying.result()

    def exception(self) -> BaseException | None:
        return self._underlying.exception()

    def __await__(self) -> ta.Generator[ta.Any, None, T]:
        return self._underlying.__await__()


def new_future() -> Future:
    return Future(_underlying=asyncio.Future())


##


class Task(Future[T]):
    def __init__(self, *, _underlying: asyncio.Task[T]) -> None:
        super().__init__(_underlying=_underlying)

    _underlying: asyncio.Task[T]


def current_task() -> Task | None:
    ut = asyncio.current_task()
    if ut is None:
        return None
    return Task(_underlying=ut)


def create_task(coro, name=None, context=None) -> Task:
    return Task(_underlying=asyncio.create_task(coro, name=name, context=context))


##


class TimerHandle:
    def __init__(self, *, _underlying: asyncio.TimerHandle) -> None:
        super().__init__()
        self._underlying = _underlying

    def cancel(self) -> None:
        return self._underlying.cancel()


def call_later(
        delay: float,
        callback: ta.Callable
) -> TimerHandle:
    return TimerHandle(_underlying=asyncio.get_running_loop().call_later(delay, callback))


##


async def run_in_executor(fn, *args):
    return await asyncio.get_running_loop().run_in_executor(None, fn, *args)


##


def gather(*coros_or_futures, return_exceptions=False):
    return asyncio.gather(
        *[obj._underlying if isinstance(obj, Future) else obj for obj in coros_or_futures],
        return_exceptions=return_exceptions,
    )


ALL_COMPLETED = asyncio.ALL_COMPLETED
FIRST_COMPLETED = asyncio.FIRST_COMPLETED
FIRST_EXCEPTION = asyncio.FIRST_EXCEPTION


def wait(fs, *, timeout=None, return_when=ALL_COMPLETED):
    return asyncio.wait(
        [obj._underlying if isinstance(obj, Future) else obj for obj in fs],
        timeout=timeout,
        return_when=return_when,
    )


def wait_for(fut, timeout):
    return asyncio.wait_for(fut._underlying if isinstance(fut, Future) else fut, timeout=timeout)


##


class Loop:
    def __init__(self, *, _underlying: asyncio.AbstractEventLoop) -> None:
        super().__init__()
        self._underlying = _underlying

    def add_signal_handler(self, sig, callback, *args):
        return self._underlying.add_signal_handler(sig, callback, *args)

    def call_soon_threadsafe(self, callback, *args, context=None):
        return self._underlying.call_soon_threadsafe(callback, *args, context=context)

    def create_future(self) -> Future:
        return Future(_underlying=self._underlying.create_future())

    def run_in_executor(self, executor, func, *args):
        return self._underlying.run_in_executor(executor, func, *args)

    def run_until_complete(self, future):
        return self._underlying.run_until_complete(future)


def get_running_loop() -> Loop:
    return Loop(_underlying=asyncio.get_running_loop())


def run_coroutine_threadsafe(coro, loop: Loop):
    return asyncio.run_coroutine_threadsafe(coro, loop._underlying)


def set_loop_eager_task_factory(loop: Loop):
    if hasattr(asyncio, "eager_task_factory"):
        loop._underlying.set_task_factory(asyncio.eager_task_factory)


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

