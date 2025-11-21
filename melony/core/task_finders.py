import importlib

from typing import Callable


def find_task_func(func_path: str) -> Callable:
    try:
        return _find_func(func_path)
    except (ImportError, AttributeError, ValueError) as exc:
        raise ImportError(f"Cannot import function '{func_path}': {exc}")
    # TODO: check that its Melony Task

def _find_func(func_path: str) -> Callable:
    module_name, func_name = func_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    func = getattr(module, func_name)
    
    if not callable(func):
        raise ValueError(f"'{func_name}' is not callable")
        
    return func