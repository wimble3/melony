import importlib

from typing import Callable


def find_task_func(func_path: str) -> Callable:
    try:
        module_name, func_name = func_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        func = getattr(module, func_name)
        
        if not callable(func):
            raise ValueError(f"'{func_name}' is not callable")
            
        return func
    except (ImportError, AttributeError, ValueError) as e:
        raise ImportError(f"Cannot import function '{func_path}': {e}")