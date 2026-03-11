from __future__ import annotations

import ctypes
from typing import Optional


class _SystemPowerStatus(ctypes.Structure):
    _fields_ = [
        ("ACLineStatus", ctypes.c_byte),
        ("BatteryFlag", ctypes.c_byte),
        ("BatteryLifePercent", ctypes.c_byte),
        ("Reserved1", ctypes.c_byte),
        ("BatteryLifeTime", ctypes.c_uint32),
        ("BatteryFullLifeTime", ctypes.c_uint32),
    ]


def is_on_ac_power() -> Optional[bool]:
    if not hasattr(ctypes, "windll"):
        return None

    status = _SystemPowerStatus()
    try:
        ok = ctypes.windll.kernel32.GetSystemPowerStatus(ctypes.byref(status))
    except Exception:
        return None

    if ok == 0:
        return None
    if status.ACLineStatus == 1:
        return True
    if status.ACLineStatus == 0:
        return False
    return None
