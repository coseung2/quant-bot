"""Pure sizing and target rules for cyclic position research.

These functions do not submit orders or change the live bot's behaviour.
"""
from decimal import Decimal, InvalidOperation, ROUND_DOWN


def _number(value, name):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid {name}") from exc
    if not result.is_finite():
        raise ValueError(f"invalid {name}")
    return result


def available_size(maximum, current, step):
    """Return remaining simultaneous-position capacity rounded down to a lot step."""
    maximum = _number(maximum, "maximum")
    current = _number(current, "current")
    step = _number(step, "step")
    if maximum <= 0 or current < 0 or step <= 0:
        raise ValueError("invalid sizing inputs")
    remaining = max(Decimal(0), maximum - current)
    return (remaining / step).to_integral_value(rounding=ROUND_DOWN) * step


def pyramid_target(side, extreme, atr, offset_atr=Decimal("0.2")):
    """Return an unrounded target just inside the recorded favourable extreme."""
    extreme = _number(extreme, "extreme")
    atr = _number(atr, "atr")
    offset = _number(offset_atr, "offset")
    if side not in ("long", "short") or extreme <= 0 or atr <= 0 or offset < 0:
        raise ValueError("invalid target inputs")
    target = extreme - offset * atr if side == "long" else extreme + offset * atr
    if target <= 0:
        raise ValueError("nonpositive target")
    return target
