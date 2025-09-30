from __future__ import annotations

import abc
import asyncio
import sys
import typing as ta


T = ta.TypeVar('T')
T_co = ta.TypeVar('T_co', covariant=True)


CancelledError = asyncio.CancelledError
QueueEmpty = asyncio.QueueEmpty
TimeoutError = asyncio.TimeoutError  # noqa


##


def _dump_tasks():
    print('\n'.join(sorted(map(repr, asyncio.all_tasks()))) + '\n', file=sys.stderr)


# import threading
#
#
# def _bomb():
#     import time
#     time.sleep(5)
#     # breakpoint()
#
#
# threading.Thread(target=_bomb).start()


##


class Future(ta.Protocol[T]):
    def cancel(self) -> bool: ...
    def add_done_callback(self, fn) -> None: ...
    def remove_done_callback(self, fn) -> int: ...
    def set_result(self, result: T) -> None: ...
    def cancelled(self) -> bool: ...
    def done(self) -> bool: ...
    def result(self) -> T: ...
    def exception(self) -> BaseException | None: ...
    def __await__(self) -> ta.Generator[ta.Any, None, T]: ...


##


class Task(Future[T]):
    def __init__(self, *, _underlying: asyncio.Task[T]) -> None:
        super().__init__()
        self._underlying = _underlying

    _underlying: asyncio.Task[T]

    def cancel(self) -> bool:
        return self._underlying.cancel()

    def add_done_callback(self, fn) -> None:
        return self._underlying.add_done_callback(fn)

    def remove_done_callback(self, fn) -> int:
        return self._underlying.remove_done_callback(fn)

    def set_result(self, result: T) -> None:
        return self._underlying.set_result(result)

    def cancelled(self) -> bool:
        return self._underlying.cancelled()

    def done(self) -> bool:
        return self._underlying.done()

    def result(self) -> T:
        return self._underlying.result()

    def exception(self) -> BaseException | None:
        return self._underlying.exception()

    def __await__(self):
        return self._underlying.__await__()


class TimerHandle:
    def __init__(self, *, _underlying: asyncio.TimerHandle) -> None:
        super().__init__()
        self._underlying = _underlying

    def cancel(self) -> None:
        return self._underlying.cancel()


class Loop:
    def __init__(self, *, _underlying: asyncio.AbstractEventLoop) -> None:
        super().__init__()
        self._underlying = _underlying

    def add_signal_handler(self, sig, callback, *args):
        return self._underlying.add_signal_handler(sig, callback, *args)

    def call_soon_threadsafe(self, callback, *args, context=None):
        return self._underlying.call_soon_threadsafe(callback, *args, context=context)

    def create_future(self) -> Future:
        return self._underlying.create_future()

    def run_in_executor(self, executor, func, *args):
        return self._underlying.run_in_executor(executor, func, *args)

    def run_until_complete(self, future):
        return self._underlying.run_until_complete(future)


class Event:
    def __init__(self, *, _underlying: asyncio.Event) -> None:
        super().__init__()
        self._underlying = _underlying

    def is_set(self):
        return self._underlying.is_set()

    def set(self):
        return self._underlying.set()

    def clear(self):
        return self._underlying.clear()

    async def wait(self):
        return await self._underlying.wait()


class Lock:
    def __init__(self, *, _underlying: asyncio.Lock) -> None:
        super().__init__()
        self._underlying = _underlying

    def locked(self):
        return self._underlying.locked()

    async def acquire(self):
        return await self._underlying.acquire()

    def release(self):
        return self._underlying.release()


class Queue(ta.Generic[T]):
    def __init__(self, *, _underlying: asyncio.Queue[T]) -> None:
        super().__init__()
        self._underlying = _underlying

    async def get(self):
        return await self._underlying.get()

    def task_done(self):
        return self._underlying.task_done()

    def put_nowait(self, item):
        return self._underlying.put_nowait(item)

    async def join(self):
        return await self._underlying.join()

    def empty(self):
        return self._underlying.empty()

    async def put(self, item):
        return await self._underlying.put(item)


ALL_COMPLETED = asyncio.ALL_COMPLETED
FIRST_COMPLETED = asyncio.FIRST_COMPLETED
FIRST_EXCEPTION = asyncio.FIRST_EXCEPTION


##


_WRAPPED_TASK_ATTR = '__textual_task__'


def _wrap_task(obj):
    if not isinstance(obj, asyncio.Task):
        return obj
    try:
        return getattr(obj, _WRAPPED_TASK_ATTR)
    except AttributeError:
        pass
    tsk = Task(_underlying=obj)
    setattr(obj, _WRAPPED_TASK_ATTR, tsk)
    return tsk


def _unwrap_future(obj):
    if isinstance(obj, Task):
        return obj._underlying
    if isinstance(obj, asyncio.Task):
        raise TypeError(obj)
    return obj


def current_task() -> Task | None:
    ut = asyncio.current_task()
    if ut is None:
        return None
    return _wrap_task(ut)


def create_task(coro, name=None, context=None) -> Task:
    return _wrap_task(asyncio.create_task(coro, name=name, context=context))


def call_later(
        delay: float,
        callback: ta.Callable
) -> TimerHandle:
    return TimerHandle(_underlying=asyncio.get_running_loop().call_later(delay, callback))


def gather(*coros_or_futures, return_exceptions=False):  # -> Future[list[T]]
    return asyncio.gather(
        *coros_or_futures,
        return_exceptions=return_exceptions,
    )


async def wait(fs, *, timeout=None, return_when=ALL_COMPLETED):  # -> (done: [Future[T]], pending: [Future[T]])
    return await asyncio.wait(
        fs,
        timeout=timeout,
        return_when=return_when,
    )


def wait_for(fut, timeout):  # -> T
    return asyncio.wait_for(
        fut,
        timeout=timeout,
    )


def get_running_loop() -> Loop:
    return Loop(_underlying=asyncio.get_running_loop())


async def run_in_executor(fn, *args):
    return await asyncio.get_running_loop().run_in_executor(None, fn, *args)


def run_coroutine_threadsafe(coro, loop: Loop):
    return asyncio.run_coroutine_threadsafe(coro, loop._underlying)


def set_loop_eager_task_factory(loop: Loop):
    if hasattr(asyncio, "eager_task_factory"):
        loop._underlying.set_task_factory(asyncio.eager_task_factory)


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


def shield(arg):
    return asyncio.shield(arg)


def new_event() -> Event:
    return Event(_underlying=asyncio.Event())


def new_lock() -> Lock:
    return Lock(_underlying=asyncio.Lock())


def new_queue() -> Queue:
    return Queue(_underlying=asyncio.Queue())


def sleep(delay: float) -> ta.Awaitable[None]:
    return asyncio.sleep(delay)


def new_future() -> Future:
    return asyncio.Future()


