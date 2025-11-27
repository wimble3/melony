import json

from datetime import datetime
from dataclasses import asdict, dataclass, field
from inspect import signature, unwrap
from typing import Callable, Any, TYPE_CHECKING, TypeVar, ParamSpec, Awaitable, final
from typing_extensions import Final
from uuid import uuid4


if TYPE_CHECKING:
    from melony.core.brokers import BaseBroker


_TaskParams = ParamSpec("_TaskParams")
_TaskResult = TypeVar("_TaskResult")
_MAX_COUNTDOWN_SEC: Final[int] = 900
_MAX_COUNTDOWN_MIN: Final[float] = _MAX_COUNTDOWN_SEC / 60


@dataclass(kw_only=True)
class _TaskMeta:
    retries_left: int | None = None
    timestamp: float = field(default_factory=lambda: datetime.timestamp(datetime.now()))


@dataclass(frozen=True, kw_only=True)
class _BaseTask:
    task_id: str
    kwargs: dict[str, Any]
    countdown: int
    retries: int
    retry_timeout: int

    _meta: _TaskMeta = field(default_factory=_TaskMeta)

    @final
    def __post_init__(self) -> None:
        self._validate_countdown()
        self._set_retries_left_to_meta()

    @final
    @property
    def timestamp(self) -> float:
        return self._meta.timestamp

    @final
    def get_execution_timestamp(self) -> float:
        return self.timestamp + self.countdown

    @final
    def _validate_countdown(self) -> None:
        if self.countdown < 0:
            raise ValueError("Countdown cannot be negative")
        elif self.countdown >= _MAX_COUNTDOWN_SEC:
            raise ValueError(
                f"Countdown cannot be greater than {_MAX_COUNTDOWN_SEC} "
                f"({_MAX_COUNTDOWN_MIN} minutes)"
            )

    @final
    def _set_retries_left_to_meta(self) -> None:
        if not self._meta.retries_left:
            self._meta.retries_left = self.retries


@dataclass(frozen=True, kw_only=True)
class TaskJSONSerializable(_BaseTask):
    func_name: str
    func_path: str


@dataclass(frozen=True, kw_only=True)
class Task(_BaseTask):
    func: Callable
    func_path: str
    broker: "BaseBroker"

    async def execute(self) -> Any:
        unwrapped_func = unwrap(self.func)
        task_result = await unwrapped_func(**self.kwargs)
        return task_result

    def as_json_serializable_obj(self) -> TaskJSONSerializable:
        return TaskJSONSerializable(
            task_id=self.task_id,
            kwargs=self.kwargs,
            countdown=self.countdown,
            func_name=self.func.__name__,
            func_path=self.func_path,
            retries=self.retries,
            retry_timeout=self.retry_timeout,
            _meta=self._meta
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self.as_json_serializable_obj())

    def as_json(self) -> str:
        json_str = json.dumps(self.as_dict())
        return json_str


class TaskWrapper(Awaitable):
    def __init__(
        self,
        func: Callable[_TaskParams, _TaskResult],
        broker: "BaseBroker",
        bound_args: dict[str, Any],
        retries: int,
        retry_timeout: int,
    ) -> None:
        self._func = func
        self._broker = broker
        self._sig = signature(func)
        self._bound_args = bound_args or {}
        self._func_path = f"{func.__module__}.{func.__qualname__}"
        self._retries = retries
        self._retry_timeout = retry_timeout

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
            countdown=countdown,
            retries=self._retries,
            retry_timeout=self._retry_timeout
        )
        await self._broker.publisher.push(task)
        return task
