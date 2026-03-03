import pytest

from melony.core.task_finders import find_task_func


def test_find_task_func_returns_callable_from_stdlib():
    func = find_task_func("json.loads")
    assert callable(func)


def test_find_task_func_returns_correct_function():
    import json
    func = find_task_func("json.loads")
    assert func is json.loads


def test_find_task_func_nonexistent_module_raises_import_error():
    with pytest.raises(ImportError, match="Cannot import"):
        find_task_func("nonexistent_module_xyz.some_func")


def test_find_task_func_nonexistent_attribute_raises_import_error():
    with pytest.raises(ImportError, match="Cannot import"):
        find_task_func("json.nonexistent_function_xyz")


def test_find_task_func_non_callable_raises_import_error():
    # sys.version is a string, not callable
    with pytest.raises(ImportError, match="Cannot import"):
        find_task_func("sys.version")


def test_find_task_func_path_without_dot_raises_import_error():
    with pytest.raises(ImportError, match="Cannot import"):
        find_task_func("nodotpath")
