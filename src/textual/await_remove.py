"""
An *optionally* awaitable object returned by methods that remove widgets.
"""

from __future__ import annotations

from typing import Generator

import rich.repr

from textual import _async
from textual._callback import invoke
from textual._debug import get_caller_file_and_line
from textual._types import CallbackType


@rich.repr.auto
class AwaitRemove:
    """An awaitable that waits for nodes to be removed."""

    def __init__(
        self, tasks: list[_async.Task], post_remove: CallbackType | None = None
    ) -> None:
        self._tasks = tasks
        self._post_remove = post_remove
        self._caller = get_caller_file_and_line()

    def __rich_repr__(self) -> rich.repr.Result:
        yield "tasks", self._tasks
        yield "post_remove", self._post_remove
        yield "caller", self._caller, None

    async def __call__(self) -> None:
        await self

    def __await__(self) -> Generator[None, None, None]:
        current_task = _async.get().current_task()
        tasks = [task for task in self._tasks if task is not current_task]

        async def await_prune() -> None:
            """Wait for the prune operation to finish."""
            await _async.get().gather(*tasks)
            if self._post_remove is not None:
                await invoke(self._post_remove)

        return await_prune().__await__()
