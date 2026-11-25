"""Incremental exact/proximity wave counts over a sliding 15-minute window.

Blocks compose a finite-state counter, so evicting the left edge does not carry
an old window's armed state forward. No trading state or exchange access lives here.
"""
from bisect import bisect_left

import numpy as np
import pandas as pd


BAR_NS = 900_000_000_000
BLOCK_SIZE = 64


def _same(a, b):
    return (a == b) | (np.isnan(a) & np.isnan(b))


class WaveBlock:
    def __init__(self, order):
        self.order = order
        self.events = {}
        self.states = tuple(range(order + 2))
        self.counts = (0,) * (order + 2)
        self.dirty = False
        self.last = -1
        self.leaves = {}
        for touch in (False, True):
            for pivot in (False, True):
                states, counts = [], []
                for state in range(order + 2):
                    armed = state == 0 or (state == order + 1 and pivot)
                    states.append((1 if touch else 0) if armed else min(state + 1, order + 1))
                    counts.append(int(armed and touch))
                self.leaves[touch, pivot] = tuple(states), tuple(counts)

    def _extend(self, event):
        states, counts = self.leaves[event]
        self.counts = tuple(c + counts[s] for s, c in zip(self.states, self.counts))
        self.states = tuple(states[s] for s in self.states)

    def put(self, slot, event):
        event = bool(event[0]), bool(event[1])
        if self.events.get(slot) == event:
            return
        if slot > self.last and not self.dirty:
            self._extend(event)
        else:
            self.dirty = True
        self.events[slot] = event
        self.last = max(self.last, slot)

    def summary(self):
        if self.dirty:
            self.states = tuple(range(self.order + 2))
            self.counts = (0,) * (self.order + 2)
            for slot in sorted(self.events):
                self._extend(self.events[slot])
            self.dirty = False
        return self.states, self.counts

    def apply(self, state, left, right, suppress_until):
        keys = sorted(self.events)
        if left <= keys[0] and keys[-1] <= right and keys[0] >= suppress_until:
            states, counts = self.summary()
            return states[state], counts[state], 0
        count = examined = 0
        for slot in keys[bisect_left(keys, left):]:
            if slot > right:
                break
            touch, pivot = self.events[slot]
            states, counts = self.leaves[touch, pivot and slot >= suppress_until]
            count += counts[state]
            state = states[state]
            examined += 1
        return state, count, examined


class WaveIndex:
    def __init__(self, order, buffer):
        self.order, self.buffer = order, buffer
        self.blocks = {}
        self.generation = -1
        self.signature = None
        self.lines = {}
        self.fallback = None
        self.value = 0

    @staticmethod
    def bounds(zone, timestamps):
        low = np.full(len(timestamps), min(float(zone['low']), float(zone['high'])))
        high = np.full(len(timestamps), max(float(zone['low']), float(zone['high'])))
        values, times = zone.get('_line_values'), zone.get('_line_timestamps')
        if zone.get('kind') == 'cloud_line' and values is not None and times is not None and len(values):
            positions = times.searchsorted(timestamps, side='right') - 1
            known = positions >= 0
            low[known] = high[known] = np.asarray(values, dtype=float)[np.minimum(positions[known], len(values)-1)]
        return low, high

    def flags(self, book, zone, indices, side):
        low, high = self.bounds(zone, book.times[indices])
        exact = zone.get('kind') == 'ob'
        touches = ((book.high[indices] >= low) & (book.low[indices] <= high)) if exact else (
            (book.high[indices] >= low-self.buffer) & (book.low[indices] <= high+self.buffer))
        pivots = np.zeros(len(indices), dtype=bool)
        valid = indices >= 2*self.order
        ends = indices[valid]
        centers = ends-self.order
        if len(ends):
            lower, upper = self.bounds(zone, book.times[centers])
            offsets = ends[:, None] + np.arange(-2*self.order, 1)
            support = side == 'support'
            values = (book.high if support else book.low)[offsets]
            values = np.where(np.isnan(values), -np.inf if support else np.inf, values)
            if support:
                result = (book.high[centers] >= values.max(axis=1)) & (book.high[centers] > upper+self.buffer)
                if exact:
                    result &= book.low[centers] > upper
            else:
                result = (book.low[centers] <= values.min(axis=1)) & (book.low[centers] < lower-self.buffer)
                if exact:
                    result &= book.high[centers] < lower
            pivots[valid] = result
        return touches, pivots

    def count(self, book, zone, side):
        active = zone.get('active_from')
        start = int(book.times.searchsorted(pd.Timestamp(active))) if active is not None else 0
        if start >= len(book.times):
            return 0
        dynamic = zone.get('kind') == 'cloud_line' and zone.get('_line_values') is not None and len(zone['_line_values'])
        signature = (zone.get('kind'), side, str(active), None if dynamic else (float(zone['low']), float(zone['high'])))
        lines = {int(t.value): float(v) for t, v in zip(zone.get('_line_timestamps', ()), zone.get('_line_values', ()))} if dynamic else {}
        fallback = (float(zone['low']), float(zone['high']))
        reset = self.signature != signature
        if self.generation == book.generation and not reset and lines == self.lines and fallback == self.fallback:
            return self.value
        if reset:
            self.blocks.clear()
            indices = np.arange(start, len(book.times))
            book.stats['wave_bootstrap_bars'] += len(indices)
        else:
            dirty = set(book.dirty_slots)
            changed_lines = [t for t in lines.keys() | self.lines.keys() if lines.get(t) != self.lines.get(t)]
            if changed_lines:
                first = min(changed_lines)
                begin = int(book.times.searchsorted(pd.Timestamp(first, unit='ns', tz='UTC')))
                dirty.update(int(s) for s in book.slots[max(0, begin):])
            if dynamic and fallback != self.fallback and lines:
                boundary = min(lines)
                dirty.update(int(s) for s in book.slots[book.times.asi8 < boundary])
            affected = {slot+d for slot in dirty for d in range(2*self.order+1)}
            indices = np.array(sorted(book.positions[s] for s in affected if s in book.positions and book.positions[s] >= start), dtype=int)
            # A shifted activation boundary can reveal a previously uncached suffix.
            missing = [i for i in range(start, min(start+2*self.order+1, len(book.slots)))
                       if int(book.slots[i]) not in self.blocks.get(int(book.slots[i])//BLOCK_SIZE, _EMPTY_BLOCK).events]
            if missing:
                indices = np.unique(np.r_[indices, missing])
            book.stats['wave_updated_bars'] += len(indices)
        if len(indices):
            touches, pivots = self.flags(book, zone, indices, side)
            for slot, touch, pivot in zip(book.slots[indices], touches, pivots):
                block_id = int(slot)//BLOCK_SIZE
                if block_id not in self.blocks:
                    self.blocks[block_id] = WaveBlock(self.order)
                self.blocks[block_id].put(int(slot), (touch, pivot))
        left, right = int(book.slots[start]), int(book.slots[-1])
        self.blocks = {k:v for k,v in self.blocks.items() if left//BLOCK_SIZE <= k <= right//BLOCK_SIZE}
        state = total = 0
        for key in sorted(self.blocks):
            state, count, examined = self.blocks[key].apply(state, left, right, left+2*self.order)
            total += count
            book.stats['wave_boundary_events'] += examined
            book.stats['wave_blocks_applied'] += 1
        self.signature, self.lines, self.fallback = signature, lines, fallback
        self.generation, self.value = book.generation, total
        return total


class _EmptyBlock:
    events = {}


_EMPTY_BLOCK = _EmptyBlock()


class WaveBook:
    def __init__(self, order=2, buffer=300.):
        self.order, self.buffer = order, buffer
        self.times = pd.DatetimeIndex([], tz='UTC')
        self.slots = np.array([], dtype=np.int64)
        self.high = self.low = np.array([], dtype=float)
        self.positions, self.indices = {}, {}
        self.generation = 0
        self.dirty_slots = set()
        self.used = set()
        self.stats = {}

    def advance(self, frame):
        times = pd.DatetimeIndex(pd.to_datetime(frame.timestamp, utc=True)).as_unit('ns')
        slots = times.asi8//BAR_NS
        if len(times) and (np.any(times.asi8 % BAR_NS) or np.any(np.diff(slots) != 1)):
            raise ValueError('wave input must be contiguous, unique closed 15m bars')
        high, low = frame.high.to_numpy(float), frame.low.to_numpy(float)
        common, old, new = np.intersect1d(self.slots, slots, return_indices=True)
        changed = common[~(_same(self.high[old], high[new]) & _same(self.low[old], low[new]))]
        added = np.setdiff1d(slots, self.slots)
        self.dirty_slots = set(int(s) for s in np.r_[changed, added])
        self.times, self.slots, self.high, self.low = times, slots, high.copy(), low.copy()
        self.positions = {int(s):i for i,s in enumerate(slots)}
        self.generation += 1
        self.used = set()
        self.stats = {'wave_bootstrap_bars':0, 'wave_updated_bars':0, 'wave_boundary_events':0,
                      'wave_blocks_applied':0, 'new_15m_bars':len(added), 'revised_15m_bars':len(changed),
                      'historical_full_replays':0}

    def count(self, zone, side, proximity=False):
        if not len(self.times):
            return 0
        if proximity and zone.get('kind') == 'ob':
            zone = {**zone, 'kind':'entry_proximity', 'active_from':zone.get('entry_wave_from', zone.get('active_from'))}
        key = (str(zone.get('zone_id') or zone.get('name')), zone.get('role_epoch'), side, zone.get('kind'))
        self.used.add(key)
        if key not in self.indices:
            self.indices[key] = WaveIndex(self.order, self.buffer)
        return self.indices[key].count(self, zone, side)

    def finish(self):
        self.indices = {k:v for k,v in self.indices.items() if k in self.used}
