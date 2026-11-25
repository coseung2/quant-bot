"""Causal moving Ichimoku boundaries for breakout attenuation only."""
import pandas as pd


def build_cloud_policy_zones(frames, zone_type):
    zones = []
    for tf in ("4h", "1d"):
        frame = frames[tf].copy()
        midpoint = lambda n: (frame.high.rolling(n).max() + frame.low.rolling(n).min()) / 2
        a = ((midpoint(9) + midpoint(26)) / 2).shift(26)
        b = midpoint(52).shift(26)
        frame["top"] = pd.concat([a, b], axis=1).max(axis=1)
        frame["bottom"] = pd.concat([a, b], axis=1).min(axis=1)
        frame["polarity"] = a >= b
        frame = frame.loc[a.notna() & b.notna()].copy()
        if frame.empty:
            continue
        groups = list(frame.groupby(frame.polarity.ne(frame.polarity.shift()).cumsum()))
        for i, (_, segment) in enumerate(groups):
            first = segment.iloc[0]
            anchor = first.timestamp
            ends = groups[i + 1][1].iloc[0].timestamp if i + 1 < len(groups) else None
            polarity = "bull" if first.polarity else "bear"
            for boundary, side in [("top", "support"), ("bottom", "resistance")]:
                values = tuple(float(v) for v in segment[boundary])
                zones.append(zone_type(
                    zone_id=f"{tf}:cloud:{polarity}:{anchor.isoformat()}:{boundary}",
                    source_tf=tf, original_side=side, low=values[0], high=values[0],
                    active_from=anchor, anchor_time=anchor, kind="cloud_line",
                    boundary_times=tuple(segment.timestamp), boundary_values=values,
                    ends_at=ends,
                ))
    return zones
