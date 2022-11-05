import mmh3
import numpy as np
import pandas as pd

DAYS_TO_MILLIS = 86400000


def convert(value, type):
    """Convert partition values by iceberg type"""
    if type == "int":
        return int.from_bytes(value, "little")
    elif type == "string":
        return value.decode()
    elif type.startswith("decimal"):
        # might be better to carry int and scale instead of
        # suffering float errors
        scale = int(type.rsplit(",", 1)[-1].rstrip(")"))
        return int.from_bytes(value, "little") / 10**scale
    elif type == "date":
        return np.array(
            int.from_bytes(value, "little") * DAYS_TO_MILLIS, dtype="M8[ms]"
        )
    else:
        raise TypeError


def typemap(typestr):
    """Convert iceberg types to pandas"""
    tm = {
        "int": pd.Int32Dtype(),
        "long": pd.Int64Dtype(),
        "boolean": "boolean",
        "string": "O",
        "uuid": "O",
        "double": "f8",
        "float": "f4",
        "timestamp": "M8[ns]",
        "timestampz": "M8[ns]",
        "time": pd.Int64Dtype(),
        "date": "M8[ns]",
    }
    if typestr in tm:
        return tm[typestr]
    if typestr.startswith("decimal"):
        return "f8"
    raise NotImplementedError


def transform(val, trans):
    """Implement partition transforms"""
    if isinstance(val, list):
        return [transform(_, trans) for _ in val]
    if trans == "identity":
        return val
    if trans.startswith("bucket"):
        N = int(trans[7:-1])
        return (mmh3.hash(val, signed=True) & 0x7FFFFFFF) % N
    if trans == "void":
        return None
    if trans.startswith("truncate"):
        W = int(trans[9:-1])
        if isinstance(val, int):
            return val - (val % W)
        elif isinstance(val, (str, bytes)):
            return val[:W]
    if trans == "day":
        if val.dtype == "M8[ms]":
            return val.view("int64") // DAYS_TO_MILLIS
        if val.dtype == "m8[ns]":
            return val.view("int64") // DAYS_TO_MILLIS // 1000000

    raise NotImplementedError(
        f"Transform for value <{val}> " f"with transformer <{trans}>"
    )
