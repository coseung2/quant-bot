"""Single-owner incremental market context, independent of order/account state."""
from bisect import bisect_left, bisect_right, insort
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
import hashlib
import json
from types import MappingProxyType
import time

import numpy as np
import pandas as pd

from bot1_wave_state import WaveBook
from zone_role_attenuation import Zone, TransitionEvent, _candidate_events, _prepare_frames


DELTAS = {'15m':pd.Timedelta(minutes=15), '1h':pd.Timedelta(hours=1),
          '4h':pd.Timedelta(hours=4), '1d':pd.Timedelta(days=1)}
COLUMNS = ['open', 'high', 'low', 'close', 'volume']


def settings_key(bot):
    values = {k:getattr(bot,k) for k in ('SYMBOL','ATR_LEN','HUNT_BUFFER','ZONE_WAVE_PIVOT_ORDER',
              'ZONE_MAX_VALID_TOUCHES','MONTHS_BACK')}
    values['rule'] = asdict(bot.ZONE_ROLE_BREAKOUT_RULE)
    values['policy'] = asdict(bot.ZONE_ROLE_POLICY_SETTING)
    return json.dumps(values,sort_keys=True)


def input_key(frames, visible_at, settings):
    digest=hashlib.sha256(settings.encode())
    digest.update(str(pd.Timestamp(visible_at).value).encode())
    for tf in DELTAS:
        frame=frames[tf]
        digest.update(tf.encode())
        digest.update(pd.DatetimeIndex(pd.to_datetime(frame.timestamp,utc=True)).as_unit('ns').asi8.tobytes())
        digest.update(frame[COLUMNS].to_numpy(dtype=float).tobytes())
    return digest.hexdigest()


class FrameWindow:
    def __init__(self, tf):
        self.tf=tf
        self.times=np.array([],dtype=np.int64)
        self.values=np.empty((0,5))
        self.revision=0
        self.frame=None
        self.added=self.revised=self.evicted=set()

    def advance(self, frame):
        frame=frame.copy().reset_index(drop=True)
        frame['timestamp']=pd.to_datetime(frame.timestamp,utc=True).astype('datetime64[ns, UTC]')
        times=pd.DatetimeIndex(frame.timestamp).as_unit('ns').asi8
        values=frame[COLUMNS].to_numpy(float)
        if not len(times) or np.any(np.diff(times)!=DELTAS[self.tf].value) or not np.isfinite(values).all():
            raise ValueError(f'invalid or discontinuous {self.tf} input')
        common,old,new=np.intersect1d(self.times,times,return_indices=True)
        self.revised=set(int(x) for x in common[np.any(self.values[old]!=values[new],axis=1)])
        self.added=set(int(x) for x in np.setdiff1d(times,self.times))
        self.evicted=set(int(x) for x in np.setdiff1d(self.times,times))
        if self.added or self.revised or self.evicted:
            self.revision+=1
        self.times,self.values,self.frame=times,values.copy(),frame
        return self


class OrderBlockBook:
    def __init__(self, tf, extract):
        self.tf,self.extract=tf,extract
        self.records={}
        self.closed=set()
        self.delta=DELTAS[tf].value

    @staticmethod
    def broken(ob, closes):
        return closes<float(ob['low']) if ob['side']=='bull' else closes>float(ob['high'])

    def advance(self, window, visible_ns, stats):
        frame,times=window.frame,window.times
        positions={int(t):i for i,t in enumerate(times)}
        closed=set(int(t) for t in times if t+self.delta<=visible_ns)
        changed_closed=(window.revised & closed) | (closed-self.closed)
        affected=set()
        for stamp in window.added | window.revised:
            i=positions[stamp]
            affected.add(i)
            if i+1<len(times):affected.add(i+1)
        if window.evicted:
            affected.add(0)
        self.records={t:r for t,r in self.records.items() if t in positions and positions[t]>0}
        new_records=set()
        for i in sorted(affected):
            stamp=int(times[i])
            if i==0:
                self.records.pop(stamp,None)
                continue
            found=self.extract(frame.iloc[i-1:i+1].reset_index(drop=True))
            stats['ob_pairs_evaluated']+=1
            if not found:
                self.records.pop(stamp,None)
                continue
            ob=found[0]
            previous=self.records.get(stamp)
            if previous is None or previous['ob']!=ob:
                self.records[stamp]={'ob':ob,'broken_at':None}
                new_records.add(stamp)
        for stamp,record in self.records.items():
            ob=record['ob']
            old_break=record['broken_at']
            if stamp in new_records or (old_break is not None and old_break in window.revised):
                eligible=(times>stamp)&(times+self.delta<=visible_ns)
                candidates=times[eligible & self.broken(ob,window.values[:,3])]
                record['broken_at']=int(candidates[0]) if len(candidates) else None
                stats['ob_initialization_rows']+=int(eligible.sum())
            else:
                candidates=[t for t in changed_closed if t>stamp and (old_break is None or t<old_break)]
                if candidates:
                    indices=np.array([positions[t] for t in candidates])
                    broken=times[indices][self.broken(ob,window.values[indices,3])]
                    if len(broken):record['broken_at']=int(broken.min())
        self.closed=closed

    def valid_obs(self, window, bar_time):
        confirmed=max(int(window.times.searchsorted(pd.Timestamp(bar_time).value,side='right'))-2,0)
        cutoff=int(window.times[confirmed])
        result=[]
        for stamp,record in sorted(self.records.items()):
            if stamp>cutoff or (record['broken_at'] is not None and record['broken_at']<=cutoff):continue
            ob=dict(record['ob'])
            ob['impulse_idx']=int(window.times.searchsorted(stamp))
            ob['anchor_idx']=ob['impulse_idx']-1
            result.append(ob)
        return result

    def policy_zones(self, visible_ns):
        result=[]
        for stamp,record in sorted(self.records.items()):
            if stamp+self.delta>=visible_ns:continue
            ob=record['ob'];anchor=pd.Timestamp(ob['anchor_time'])
            result.append(Zone(zone_id=f"{self.tf}:ob:{ob['side']}:{anchor.isoformat()}",source_tf=self.tf,
                original_side='support' if ob['side']=='bull' else 'resistance',low=float(ob['low']),high=float(ob['high']),
                active_from=pd.Timestamp(stamp+self.delta,tz='UTC'),anchor_time=anchor,
                invalidated_at=pd.Timestamp(record['broken_at']+self.delta,tz='UTC') if record['broken_at'] is not None else None,
                anchor_open=float(ob['anchor_open']),anchor_high=float(ob['anchor_high']),anchor_low=float(ob['anchor_low']),
                anchor_close=float(ob['anchor_close']),impulse_time=pd.Timestamp(ob['impulse_time'])))
        return result


class CloudBook:
    def __init__(self,tf):
        self.tf=tf
        self.rows={}
        self.groups=[]

    def _append(self,stamp,values):
        top,bottom,polarity=values
        if not self.groups or self.groups[-1]['polarity']!=polarity:
            if self.groups:
                self.groups[-1]['ends']=stamp
                self.groups[-1]['cache']=None
            self.groups.append({'times':[],'top':[],'bottom':[],'polarity':polarity,'ends':None,'cache':None})
        group=self.groups[-1]
        group['times'].append(stamp);group['top'].append(top);group['bottom'].append(bottom);group['cache']=None

    def advance(self, derived, stats):
        known=derived[derived.cloud_span_a.notna() & derived.cloud_span_b.notna()]
        rows={int(r.timestamp.value):(float(r.cloud_top),float(r.cloud_bottom),bool(r.cloud_span_a>=r.cloud_span_b))
              for r in known.itertuples(index=False)}
        revised=[t for t in rows.keys() & self.rows.keys() if rows[t]!=self.rows[t]]
        added=sorted(rows.keys()-self.rows.keys())
        backwards=bool(added and self.rows and added[0]<max(self.rows))
        if revised or backwards:
            self.groups=[]
            for stamp,value in sorted(rows.items()):self._append(stamp,value)
            stats['cloud_revision_rows']+=len(rows)
        else:
            for stamp in added:self._append(stamp,rows[stamp])
            stats['cloud_appended_rows']+=len(added)
            if rows:
                first=min(rows)
                while self.groups and self.groups[0]['times'][-1]<first:self.groups.pop(0)
                if self.groups:
                    group=self.groups[0];cut=bisect_left(group['times'],first)
                    if cut:
                        for field in ('times','top','bottom'):group[field]=group[field][cut:]
                        group['cache']=None
                last=max(rows)
                # An earlier requested view is a rebuild, never a future snapshot.
                if self.groups and self.groups[-1]['times'][-1]>last:
                    self.groups=[]
                    for stamp,value in sorted(rows.items()):self._append(stamp,value)
                    stats['cloud_revision_rows']+=len(rows)
            else:self.groups=[]
        self.rows=rows

    def zones(self):
        result=[]
        for group in self.groups:
            if group['cache'] is None:
                anchor=pd.Timestamp(group['times'][0],tz='UTC')
                polarity='bull' if group['polarity'] else 'bear'
                group['cache']=tuple(Zone(zone_id=f'{self.tf}:cloud:{polarity}:{anchor.isoformat()}:{boundary}',
                    source_tf=self.tf,original_side=side,low=group[boundary][0],high=group[boundary][0],
                    active_from=anchor,anchor_time=anchor,kind='cloud_line',
                    boundary_times=tuple(pd.Timestamp(t,tz='UTC') for t in group['times']),
                    boundary_values=tuple(group[boundary]),
                    ends_at=pd.Timestamp(group['ends'],tz='UTC') if group['ends'] is not None else None)
                    for boundary,side in [('top','support'),('bottom','resistance')])
            result.extend(group['cache'])
        return result

    def native_zones(self,bar_time,format_time):
        target=pd.Timestamp(bar_time).value
        groups=[g for g in self.groups if g['times'][0]<=target]
        if not groups:return []
        group=groups[-1];end=bisect_right(group['times'],target)
        anchor=pd.Timestamp(group['times'][0],tz='UTC');polarity='bull' if group['polarity'] else 'bear'
        key=f'{self.tf}:cloud:{polarity}:{format_time(anchor)}'
        result=[]
        for role,side in [('top','support'),('bottom','resistance')]:
            value=group[role][end-1]
            result.append(dict(zone_id=f'{key}:{role}',name=f'{self.tf}_cloud_{role}',tf=self.tf,kind='cloud_line',
                side=side,low=value,high=value,cloud_role=role,cloud_polarity=polarity,segment_start=anchor,
                active_from=anchor,_line_timestamps=pd.DatetimeIndex([pd.Timestamp(t,tz='UTC') for t in group['times'][:end]]),
                _line_values=np.array(group[role][:end],dtype=float)))
        return result


class RoleIndex:
    def __init__(self):
        self.zone=None
        self.records={}
        self.by_side={'support':[],'resistance':[]}
        self.window_key=None
        self.events=[]

    def advance(self, zone, frame, changed, rule, stats):
        times=pd.DatetimeIndex(frame.close_time).as_unit('ns').asi8
        window_key=(len(times),int(times[0]),int(times[-1]))
        if not changed and self.zone==zone and self.window_key==window_key:
            return self.events
        changed=set(changed)
        if self.window_key is not None and self.window_key[1]!=window_key[1]:
            # The new first row has no previous close in a full reconstruction.
            changed.add(int(times[0]))
        self.window_key=window_key
        valid=set(int(t) for t in times if t>zone.active_from.value)
        previous=self.zone
        reset=previous is None or any(getattr(previous,k)!=getattr(zone,k) for k in
              ('original_side','low','high','active_from','kind'))
        # Moving cloud bounds retain their first value; only revised/added boundary intervals change candidates.
        boundary_dirty=set()
        if previous is not None and zone.boundary_times:
            old={t.value:v for t,v in zip(previous.boundary_times,previous.boundary_values)}
            new={t.value:v for t,v in zip(zone.boundary_times,zone.boundary_values)}
            differences=[t for t in old.keys()|new.keys() if old.get(t)!=new.get(t)]
            if differences:
                first=min(differences)
                boundary_dirty={int(t) for t,opened in zip(times,pd.DatetimeIndex(frame.timestamp).as_unit('ns').asi8) if opened>=first}
        dirty=valid if reset else ((set(changed)|boundary_dirty)&valid)
        removed=set(self.records)-valid
        for stamp in removed|dirty:
            records=self.records.pop(stamp,[])
            for record in records:
                side=record['effective_side'];keys=self.by_side[side]
                key=(stamp,0 if side=='support' else 1)
                index=bisect_left(keys,key)
                if index<len(keys) and keys[index]==key:keys.pop(index)
        if dirty:
            positions=np.array(sorted(int(times.searchsorted(t)) for t in dirty),dtype=int)
            # Include each predecessor so the existing crossing rule sees the true previous close.
            neighbors=np.unique(np.r_[positions,positions[positions>0]-1])
            subset=frame.iloc[neighbors].copy().reset_index(drop=True)
            found=_candidate_events(zone,subset,'1h',rule)
            for record in found:
                stamp=record['event_time'].value
                if stamp not in dirty:continue
                self.records.setdefault(stamp,[]).append(record)
                side=record['effective_side'];insort(self.by_side[side],(stamp,0 if side=='support' else 1))
            stats['role_bootstrap_rows' if reset else 'role_updated_rows']+=len(positions)
        self.zone=zone
        opposite='resistance' if zone.original_side=='support' else 'support'
        candidates=self.by_side[opposite]
        if not candidates:
            self.events=[]
            return self.events
        first=candidates[0]
        same=self.by_side[zone.original_side]
        position=bisect_right(same,first)
        selected=[first]+([same[position]] if position<len(same) else [])
        events=[]
        for epoch,key in enumerate(selected,1):
            side='support' if key[1]==0 else 'resistance'
            record=next(r for r in self.records[key[0]] if r['effective_side']==side)
            events.append(TransitionEvent(zone_id=zone.zone_id,event_time=record['event_time'],timeframe='1h',
                effective_side=side,direction=record['direction'],strength=record['strength'],
                event_kind='transition' if epoch==1 else 'deleted',role_epoch=epoch))
        self.events=events
        return events


def _freeze_zone(zone):
    result=dict(zone)
    if '_line_values' in result:
        values=result['_line_values'].copy();values.flags.writeable=False;result['_line_values']=values
    return MappingProxyType(result)


@dataclass(frozen=True)
class ContextSnapshot:
    key: str
    visible_at: pd.Timestamp
    generation: int
    zones: tuple
    selected: tuple
    exhausted: tuple
    stats: object

    def matches(self,frames,visible_at,bot):
        return self.visible_at==pd.Timestamp(visible_at) and self.key==input_key(frames,visible_at,settings_key(bot))


class SignalContext:
    def __init__(self,bot):
        self.bot=bot
        self.settings=None
        self.snapshot=None
        self.generation=0
        self.windows={}
        self.books={}
        self.clouds={}
        self.roles={}
        self.hourly=None
        self.wave=None

    def advance(self,frames,visible_at):
        try:
            return self._advance(frames,visible_at)
        except Exception:
            # No partially advanced cache may be reused after a failed revision.
            self.settings=None
            self.snapshot=None
            raise

    def _advance(self,frames,visible_at):
        started=time.perf_counter();bot=self.bot;visible_at=pd.Timestamp(visible_at)
        if visible_at.tzinfo is None:visible_at=visible_at.tz_localize('UTC')
        else:visible_at=visible_at.tz_convert('UTC')
        config=settings_key(bot);key=input_key(frames,visible_at,config)
        if self.snapshot is not None and self.snapshot.key==key:return self.snapshot
        reset=config!=self.settings or (self.snapshot is not None and visible_at<self.snapshot.visible_at)
        if reset:
            self.windows={tf:FrameWindow(tf) for tf in DELTAS}
            self.books={tf:OrderBlockBook(tf,bot.extract_order_blocks) for tf in ('1h','4h','1d')}
            self.clouds={tf:CloudBook(tf) for tf in ('4h','1d')}
            self.roles={};self.hourly=None
            self.wave=WaveBook(bot.ZONE_WAVE_PIVOT_ORDER,bot.HUNT_BUFFER)
            self.settings=config
        stats={'bootstrap':reset,'ob_pairs_evaluated':0,'ob_initialization_rows':0,'cloud_revision_rows':0,
               'cloud_appended_rows':0,'role_bootstrap_rows':0,'role_updated_rows':0,'input_revised_bars':0,
               'historical_full_replays':0}
        for tf in DELTAS:
            incoming=frames[tf].loc[pd.to_datetime(frames[tf].timestamp,utc=True)<=visible_at]
            if tf=='15m':incoming=incoming.loc[pd.to_datetime(incoming.timestamp,utc=True)+DELTAS[tf]<=visible_at]
            window=self.windows[tf].advance(incoming)
            stats['input_revised_bars']+=len(window.revised)
            if window.times[-1]+DELTAS[tf].value<visible_at.value:raise ValueError(f'stale {tf} input')
        live=self.windows['15m'].frame
        if pd.Timestamp(live.timestamp.iloc[-1])+DELTAS['15m']!=visible_at:raise ValueError('missing target closed candle')
        raw={tf:self.windows[tf].frame for tf in ('1h','4h','1d')}
        prepared=_prepare_frames(raw,visible_at,bot.add_atr,bot.ATR_LEN,bot.ZONE_ROLE_POLICY_SETTING)
        for tf,book in self.books.items():book.advance(self.windows[tf],visible_at.value,stats)
        for tf,cloud in self.clouds.items():cloud.advance(bot.add_ichimoku(raw[tf]),stats)
        zones=[]
        for tf in ('1h','4h','1d'):zones.extend(self.books[tf].policy_zones(visible_at.value))
        zones.sort(key=lambda z:z.active_from)
        for tf in ('4h','1d'):zones.extend(self.clouds[tf].zones())
        hourly=prepared['1h'];new=hourly.set_index('close_time')[['open','high','low','close','atr']]
        if self.hourly is None:
            changed=set(pd.DatetimeIndex(new.index).asi8)
        else:
            common=self.hourly.index.intersection(new.index)
            a,b=self.hourly.loc[common].to_numpy(),new.loc[common].to_numpy()
            equal=(a==b)|(np.isnan(a)&np.isnan(b))
            changed=set(pd.DatetimeIndex(common[np.any(~equal,axis=1)]).asi8)
            changed.update(pd.DatetimeIndex(new.index.difference(self.hourly.index)).asi8)
        # Changing a close also changes the following candle's previous-close crossing test.
        times=pd.DatetimeIndex(hourly.close_time).asi8
        positions={int(t):i for i,t in enumerate(times)}
        changed.update(int(times[positions[t]+1]) for t in list(changed) if t in positions and positions[t]+1<len(times))
        event_map={}
        for zone in zones:
            if zone.zone_id not in self.roles:self.roles[zone.zone_id]=RoleIndex()
            event_map[zone.zone_id]=self.roles[zone.zone_id].advance(zone,hourly,changed,bot.ZONE_ROLE_BREAKOUT_RULE,stats)
        self.roles={z.zone_id:self.roles[z.zone_id] for z in zones}
        self.hourly=new.copy()
        bar_time=live.timestamp.iloc[-1]
        obs={tf:self.books[tf].valid_obs(self.windows[tf],bar_time) for tf in ('1h','4h','1d')}
        clouds={tf:self.clouds[tf].native_zones(bar_time,bot.format_meta_timestamp) for tf in ('4h','1d')}
        active=bot.get_active_zones(live,raw['4h'],raw['1d'],bar_time,df_1h=raw['1h'],
            zone_role_context=(zones,event_map),role_visible_at=visible_at,valid_obs_by_tf=obs,cloud_zones_by_tf=clouds)
        self.wave.advance(live)
        selected,exhausted=bot.select_entry_lifecycle_zones(live,active,len(live)-1,wave_book=self.wave)
        self.wave.finish();stats.update(self.wave.stats)
        self.generation+=1
        stats.update(generation=self.generation,elapsed_ms=(time.perf_counter()-started)*1000,
                     cached_wave_indices=len(self.wave.indices),cached_role_indices=len(self.roles))
        if settings_key(bot)!=config:raise ValueError('settings changed during context calculation')
        snapshot=ContextSnapshot(key,visible_at,self.generation,tuple(_freeze_zone(z) for z in active),
            tuple(_freeze_zone(z) for z in selected),tuple(_freeze_zone(z) for z in exhausted),MappingProxyType(stats))
        self.snapshot=snapshot
        return snapshot


class ContextCalculator:
    """One worker owns all mutable caches; readers only receive completed snapshots."""
    def __init__(self,bot):
        self.bot=bot
        self.context=SignalContext(bot)
        self.executor=ThreadPoolExecutor(max_workers=1,thread_name_prefix='bot1-context')
        self.future=None
        self.key=None

    def submit(self,frames,visible_at):
        key=input_key(frames,visible_at,settings_key(self.bot))
        if self.future is not None:
            if self.key==key:return key
            if not self.future.done():return None
        copied={tf:f.copy(deep=True) for tf,f in frames.items()}
        self.key=key
        self.future=self.executor.submit(self.context.advance,copied,visible_at)
        return key

    def result(self,key):
        if key is None or key!=self.key or not self.future.done():return None
        snapshot=self.future.result()
        if snapshot.key!=key:raise ValueError('context input revision mismatch')
        return snapshot

    def close(self):
        self.executor.shutdown(wait=False,cancel_futures=True)
