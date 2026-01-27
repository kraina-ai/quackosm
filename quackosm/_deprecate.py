from typing import Any, Callable

from packaging import version
from pandas import __version__ as pd_version
from pandas._typing import F
from pandas.util._decorators import deprecate as pd_deprecate
from pandas.util._decorators import deprecate_kwarg as pd_deprecate_kwarg

PANDAS_ABOVE_3 = version.parse(pd_version) >= version.parse("3.0.0")


def deprecate(
    name: str, alternative: Callable[..., Any], version: str, msg: str | None = None
) -> Callable[[F], F]:
    if PANDAS_ABOVE_3:
        return pd_deprecate(  # type: ignore[no-any-return]
            name=name, alternative=alternative, version=version, klass=FutureWarning, msg=msg
        )
    else:
        return pd_deprecate(name=name, alternative=alternative, version=version, msg=msg)  # type: ignore[no-any-return]


def deprecate_kwarg(old_arg_name: str, new_arg_name: str | None) -> Callable[[F], F]:
    if PANDAS_ABOVE_3:
        return pd_deprecate_kwarg(  # type: ignore[no-any-return]
            old_arg_name=old_arg_name, new_arg_name=new_arg_name, klass=FutureWarning
        )
    else:
        return pd_deprecate_kwarg(old_arg_name=old_arg_name, new_arg_name=new_arg_name)  # type: ignore[no-any-return]
