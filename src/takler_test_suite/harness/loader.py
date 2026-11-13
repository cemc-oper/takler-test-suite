import importlib.util

from .runtime import RunError


def load_entry(entry):
    spec = importlib.util.spec_from_file_location("scenario_case", entry)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not callable(getattr(module, "run", None)):
        raise RunError("Case entry must export run()")
    return module
