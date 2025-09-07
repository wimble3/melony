import json

from datetime import datetime
from dataclasses import asdict, dataclass
from inspect import signature, unwrap
from typing import Callable, Any, TYPE_CHECKING, Sequence, TypeVar, ParamSpec, Awaitable
from uuid import uuid4
from classes import typeclass


if TYPE_CHECKING:
    from melony.core.brokers import BaseBroker


_TaskParams = ParamSpec("_TaskParams")
_TaskResult = TypeVar("_TaskResult")
_MAX_COUNTDOWN_SEC = 900
_MAX_COUNTDOWN_MIN = _MAX_COUNTDOWN_SEC / 60


@dataclass(frozen=True, kw_only=True)
class _BaseTask:
    task_id: str
    kwargs: dict[str, Any]
    countdown: int
    timestamp: float = datetime.timestamp(datetime.now())

    def get_execution_timestamp(self) -> float:
        return self.timestamp + self.countdown

    def __post_init__(self) -> None:
        self._validate_countdown()

    def _validate_countdown(self) -> None:
        if self.countdown < 0:
            raise ValueError("Countdown cannot be negative")
        elif self.countdown >= _MAX_COUNTDOWN_SEC:
            raise ValueError(
                f"Countdown cannot be greater than {_MAX_COUNTDOWN_SEC} "
                f"({_MAX_COUNTDOWN_MIN} minutes)"
            )


@dataclass(frozen=True, kw_only=True)
class TaskJSONSerializable(_BaseTask):
    func_name: str
    func_path: str


@dataclass(frozen=True, kw_only=True)
class Task(_BaseTask):
    func: Callable
    func_path: str
    broker: "BaseBroker | None" = None

    async def execute(self) -> ...:  # @@@ typing
        unwrapped_func = unwrap(self.func)
        task_result = await unwrapped_func(**self.kwargs)
        return task_result

    async def get_result(self) -> ...:  # @@@ typing
        ...

    def as_json_serializable_obj(self) -> TaskJSONSerializable:
        return TaskJSONSerializable(
            task_id=self.task_id,
            kwargs=self.kwargs,
            countdown=self.countdown,
            func_name=self.func.__name__,
            func_path=self.func_path
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self.as_json_serializable_obj())

    def as_json(self) -> str:
        return json.dumps(self.as_dict())
        

class TaskWrapper(Awaitable):
    def __init__(
            self,
            func: Callable[_TaskParams, _TaskResult],
            broker: "BaseBroker",
            bound_args: dict[str, Any]
    ) -> None:
        self._func = func
        self._broker = broker
        self._sig = signature(func)
        self._bound_args = bound_args or {}
        self._func_path = f"{func.__module__}.{func.__qualname__}"

    def __call__(
            self,
            *args: _TaskParams.args,  # type: ignore
            **kwargs: _TaskParams.kwargs
    ) -> "TaskWrapper":
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        self._bound_args = bound.arguments
        return self

    def __await__(self) -> Any:
        raise RuntimeError(
            "Task cannot be awaited, did you forget to call .delay()?"
        )

    async def delay(self, countdown: int = 0) -> Task:
        task = Task(
            task_id=str(uuid4()),
            func=self._func,
            func_path=self._func_path,
            kwargs=self._bound_args,
            broker=self._broker,
            countdown=countdown
        )
        await self._broker.push(task)
        return task


@typeclass
async def revoke() -> None:
    ...


async def _revoke_impl(instance: str) -> None:
    ...


async def _revoke_bulk_impl(instance: list[str]) -> None:
    ...


async def filter_tasks_by_execution_time(tasks: Sequence[Task]) -> None:
    ...


async def execute_tasks(tasks: Sequence[Task]) -> None:
    ...
