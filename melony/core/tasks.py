from dataclasses import dataclass
from inspect import signature
from logging import getLogger
from typing import Callable, Any, TYPE_CHECKING, TypeVar, ParamSpec, Awaitable
from uuid import uuid4


if TYPE_CHECKING:
    from melony.core.brokers import BaseBroker

logger = getLogger(__name__)
P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True)
class Task:
    task_id: str
    func: Callable[P, R]
    kwargs: dict[str, Any]
    broker: BaseBroker

    async def result(self) -> R:
        ...


class TaskWrapper(Awaitable):
    def __init__(
            self,
            func: Callable[P, R],
            broker: BaseBroker,
            bound_args: dict | None = None
    ) -> None:
        self._func = func
        self._broker = broker
        self._sig = signature(func)
        self._bound_args = bound_args or {}

    async def delay(self, countdown: int = 0) -> Task:
        task = Task(
            task_id=str(uuid4()),
            func=self._func,
            kwargs=self._bound_args,
            broker=self._broker,
        )
        await self._broker.push(task, countdown=countdown)
        return task

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> "TaskWrapper":
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        return TaskWrapper(
            func=self._func,
            broker=self._broker,
            bound_args=bound.arguments
        )

    def __await__(self) -> Any:
        raise RuntimeError(
            "Task cannot be awaited, did you forget to call .delay()?"
        )
