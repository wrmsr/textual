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


class Task(abc.ABC, ta.Generic[T]):
    @abc.abstractmethod
    def cancel(self) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def add_done_callback(self, fn) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def remove_done_callback(self, fn) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def set_result(self, result: T) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def cancelled(self) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def done(self) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def result(self) -> T:
        raise NotImplementedError

    @abc.abstractmethod
    def exception(self) -> BaseException | None:
        raise NotImplementedError

    @abc.abstractmethod
    def __await__(self):
        raise NotImplementedError


class TimerHandle(abc.ABC):
    @abc.abstractmethod
    def cancel(self) -> None:
        raise NotImplementedError


class Loop(abc.ABC):
    @abc.abstractmethod
    def add_signal_handler(self, sig, callback, *args):
        raise NotImplementedError

    @abc.abstractmethod
    def call_soon_threadsafe(self, callback, *args, context=None):
        raise NotImplementedError

    @abc.abstractmethod
    def create_future(self) -> Future:
        raise NotImplementedError

    @abc.abstractmethod
    def run_in_executor(self, executor, func, *args):
        raise NotImplementedError

    @abc.abstractmethod
    def run_until_complete(self, future):
        raise NotImplementedError


class Event:
    @abc.abstractmethod
    def is_set(self):
        raise NotImplementedError

    @abc.abstractmethod
    def set(self):
        raise NotImplementedError

    @abc.abstractmethod
    def clear(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def wait(self):
        raise NotImplementedError


class Lock:
    @abc.abstractmethod
    def locked(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def acquire(self):
        raise NotImplementedError

    @abc.abstractmethod
    def release(self):
        raise NotImplementedError


class Queue(ta.Generic[T]):
    @abc.abstractmethod
    async def get(self):
        raise NotImplementedError

    @abc.abstractmethod
    def task_done(self):
        raise NotImplementedError

    @abc.abstractmethod
    def put_nowait(self, item):
        raise NotImplementedError

    @abc.abstractmethod
    async def join(self):
        raise NotImplementedError

    @abc.abstractmethod
    def empty(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def put(self, item):
        raise NotImplementedError


ALL_COMPLETED = asyncio.ALL_COMPLETED
FIRST_COMPLETED = asyncio.FIRST_COMPLETED
FIRST_EXCEPTION = asyncio.FIRST_EXCEPTION


class Async(abc.ABC):
    @abc.abstractmethod
    def new_future(self) -> Future:
        raise NotImplementedError

    #

    @abc.abstractmethod
    def current_task(self) -> Task | None:
        raise NotImplementedError

    @abc.abstractmethod
    def create_task(self, coro, name=None, context=None) -> Task:
        raise NotImplementedError

    #

    @abc.abstractmethod
    def call_later(
            self,
            delay: float,
            callback: ta.Callable
    ) -> TimerHandle:
        raise NotImplementedError

    #

    @abc.abstractmethod
    def get_running_loop(self) -> Loop:
        raise NotImplementedError

    @abc.abstractmethod
    async def run_in_executor(self, fn, *args):
        raise NotImplementedError

    @abc.abstractmethod
    def run_coroutine_threadsafe(self, coro, loop: Loop):
        raise NotImplementedError

    @abc.abstractmethod
    def set_loop_eager_task_factory(self, loop: Loop):
        raise NotImplementedError

    #

    @abc.abstractmethod
    def gather(self, *coros_or_futures, return_exceptions=False):  # -> Future[list[T]]
        raise NotImplementedError

    @abc.abstractmethod
    async def wait(self, fs, *, timeout=None, return_when=ALL_COMPLETED):  # -> (done: [Future[T]], pending: [Future[T]])
        raise NotImplementedError

    @abc.abstractmethod
    def wait_for(self, fut, timeout):  # -> T
        raise NotImplementedError

    @abc.abstractmethod
    def run(self, main):
        raise NotImplementedError

    @abc.abstractmethod
    def run_main(self, fn):
        raise NotImplementedError

    @abc.abstractmethod
    def shield(self, arg):
        raise NotImplementedError

    @abc.abstractmethod
    def sleep(self, delay: float) -> ta.Awaitable[None]:
        raise NotImplementedError

    #

    @abc.abstractmethod
    def new_event(self) -> Event:
        raise NotImplementedError

    #

    @abc.abstractmethod
    def new_lock(self) -> Lock:
        raise NotImplementedError

    #

    @abc.abstractmethod
    def new_queue(self) -> Queue:
        raise NotImplementedError


##


class _AsyncioAsync(Async):
    def new_future(self) -> Future:
        return asyncio.Future()

    #

    class _Task(Task[T]):
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

    _WRAPPED_TASK_ATTR = '__textual_task__'

    def _wrap_task(self, obj):
        if not isinstance(obj, asyncio.Task):
            return obj
        try:
            return getattr(obj, self._WRAPPED_TASK_ATTR)
        except AttributeError:
            pass
        tsk = _AsyncioAsync._Task(_underlying=obj)
        setattr(obj, self._WRAPPED_TASK_ATTR, tsk)
        return tsk

    def _unwrap_future(self, obj):
        if isinstance(obj, _AsyncioAsync._Task):
            return obj._underlying
        if isinstance(obj, (Task, asyncio.Task)):
            raise TypeError(obj)
        return obj

    def current_task(self) -> Task | None:
        ut = asyncio.current_task()
        if ut is None:
            return None
        return self._wrap_task(ut)

    def create_task(self, coro, name=None, context=None) -> Task:
        return self._wrap_task(asyncio.create_task(coro, name=name, context=context))

    #

    class _TimerHandle(TimerHandle):
        def __init__(self, *, _underlying: asyncio.TimerHandle) -> None:
            super().__init__()
            self._underlying = _underlying

        def cancel(self) -> None:
            return self._underlying.cancel()

    def call_later(
            self,
            delay: float,
            callback: ta.Callable
    ) -> TimerHandle:
        return self._TimerHandle(_underlying=asyncio.get_running_loop().call_later(delay, callback))

    #

    class _Loop(Loop):
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

    def get_running_loop(self) -> Loop:
        return _AsyncioAsync._Loop(_underlying=asyncio.get_running_loop())

    async def run_in_executor(self, fn, *args):
        return await asyncio.get_running_loop().run_in_executor(None, fn, *args)

    def run_coroutine_threadsafe(self, coro, loop: Loop):
        if not isinstance(loop, _AsyncioAsync._Loop):
            raise TypeError(loop)
        return asyncio.run_coroutine_threadsafe(coro, loop._underlying)

    def set_loop_eager_task_factory(self, loop: Loop):
        if not isinstance(loop, _AsyncioAsync._Loop):
            raise TypeError(loop)
        if hasattr(asyncio, "eager_task_factory"):
            loop._underlying.set_task_factory(asyncio.eager_task_factory)

    #

    def gather(self, *coros_or_futures, return_exceptions=False):  # -> Future[list[T]]
        return asyncio.gather(
            *coros_or_futures,
            return_exceptions=return_exceptions,
        )

    async def wait(self, fs, *, timeout=None, return_when=ALL_COMPLETED):  # -> (done: [Future[T]], pending: [Future[T]])
        return await asyncio.wait(
            fs,
            timeout=timeout,
            return_when=return_when,
        )

    def wait_for(self, fut, timeout):  # -> T
        return asyncio.wait_for(
            fut,
            timeout=timeout,
        )

    def run(self, main):
        return asyncio.run(main)

    _ASYNCIO_GET_EVENT_LOOP_IS_DEPRECATED = sys.version_info >= (3, 10, 0)

    def run_main(self, fn):
        if self._ASYNCIO_GET_EVENT_LOOP_IS_DEPRECATED:
            # N.B. This does work with Python<3.10, but global Locks, Events, etc
            # eagerly bind the event loop, and result in Future bound to wrong
            # loop errors.
            return self.run(fn())
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
        return self.run(fn())

    def shield(self, arg):
        return asyncio.shield(arg)

    def sleep(self, delay: float) -> ta.Awaitable[None]:
        return asyncio.sleep(delay)

    #

    class _Event(Event):
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

    def new_event(self) -> Event:
        return self._Event(_underlying=asyncio.Event())

    #

    class _Lock(Lock):
        def __init__(self, *, _underlying: asyncio.Lock) -> None:
            super().__init__()
            self._underlying = _underlying

        def locked(self):
            return self._underlying.locked()

        async def acquire(self):
            return await self._underlying.acquire()

        def release(self):
            return self._underlying.release()

    def new_lock(self) -> Lock:
        return self._Lock(_underlying=asyncio.Lock())

    #

    class _Queue(Queue[T]):
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

    def new_queue(self) -> Queue:
        return self._Queue(_underlying=asyncio.Queue())


def get() -> Async:
    return _AsyncioAsync()
