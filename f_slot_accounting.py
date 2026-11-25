"""Confirmed cumulative execution accounting for independently owned F slots."""
from datetime import datetime, timezone
import math


class AccountingPending(RuntimeError):
    """Execution/settlement unavailable; retry without sending a trading order."""


def exit_reason_label(reason):
    labels = {'manual_close': '사용자 수동 종료', 'tp2_exit': '2차 익절 완료',
              'stop_loss': '손절', 'stop_after_tp1': '1차 익절 후 보호 손절',
              'tp2_out_of_order_flatten': '2차 익절 선체결 후 잔량 종료'}
    if reason in labels:
        return labels[reason]
    if reason.startswith('max_hold_'):
        return '최대 보유시간 종료'
    return '보호 주문 실패로 종료'


def confirmed_execution(order, order_id, event_type):
    if not isinstance(order, dict) or str(order.get('id') or '') != order_id:
        raise ValueError('execution order ID mismatch')
    info = order.get('info') or {}
    qty = float(order.get('filled') or info.get('cumExecQty') or 0)
    price = float(order.get('average') or info.get('avgPrice') or 0)
    fee = (order.get('fee') or {}).get('cost')
    if fee is None:
        fee = info.get('cumExecFee')
    if fee is None:
        raise ValueError('confirmed execution fee unavailable')
    fee = abs(float(fee))
    if not all(math.isfinite(x) for x in (qty, price, fee)) or qty <= 0 or price <= 0:
        raise ValueError('confirmed execution quantity/price unavailable')
    ms = order.get('lastTradeTimestamp') or info.get('updatedTime')
    if not ms:
        raise ValueError('confirmed execution timestamp unavailable')
    when = datetime.fromtimestamp(int(ms) / 1000, timezone.utc).isoformat()
    return dict(order_id=order_id, event_type=event_type, size=qty, price=price,
                fee_amount=fee, event_time=when)


def summarize_slot(pos, executions, records, final):
    by_id = {e['order_id']: e for e in executions}
    if len(by_id) != len(executions):
        raise ValueError('duplicate execution order')
    qty = sum(e['size'] for e in executions)
    if qty > pos.entry_size + 1e-8 or (final and abs(qty-pos.entry_size) > 1e-8):
        raise ValueError('slot execution quantities do not conserve entry size')
    matched = {}
    for oid, execution in by_id.items():
        rows = [r for r in records if str(r.get('orderId') or '') == oid]
        if rows and abs(sum(float(r.get('closedSize') or 0) for r in rows)-execution['size']) < 1e-8:
            matched[oid] = rows
    complete = len(matched) == len(by_id)
    allocated_entry_fee = sum(float(r['openFee']) for rows in matched.values()
                              for r in rows if r.get('openFee') is not None)
    actual_exit_fee = sum(e['fee_amount'] for e in executions)
    entry_fee = abs(float(pos.entry_fee or 0))
    fees = entry_fee + actual_exit_fee
    gross = sum((e['price']-pos.entry_price)*(1 if pos.side == 'long' else -1)*e['size']
                for e in executions)
    net = gross-fees
    source = 'confirmed_fills'
    if complete and by_id:
        net = sum(float(r['closedPnl']) for rows in matched.values() for r in rows)
        have_fees = all(r.get('openFee') is not None and r.get('closeFee') is not None
                        for rows in matched.values() for r in rows)
        if final and have_fees:
            fees = sum(abs(float(r['openFee']))+abs(float(r['closeFee']))
                       for rows in matched.values() for r in rows)
        elif not final:
            net -= entry_fee-allocated_entry_fee
        gross = net+fees
        source = 'exchange_closed_pnl'
    if not all(math.isfinite(x) for x in (fees, gross, net)):
        raise ValueError('non-finite slot accounting result')
    events = []
    for e in executions:
        rows = matched.get(e['order_id']) or []
        delta = ((e['price']-pos.entry_price)*(1 if pos.side == 'long' else -1)*e['size']
                 - e['fee_amount'])
        if rows and all(r.get('openFee') is not None for r in rows):
            delta = sum(float(r['closedPnl'])+abs(float(r['openFee'])) for r in rows)
        events.append(dict(e, pnl_delta=delta))
    return dict(gross_pnl=gross, fees_total=fees, realized_pnl=net,
                size_remaining=round(pos.entry_size-qty, 3), source=source, events=events)


def write_execution_event(conn, trade_id, event, now, note):
    kind = event['event_type']
    key = f'{trade_id}:{kind}' if kind in ('tp1', 'tp2_exit') else f'{trade_id}:{kind}:{event["order_id"]}'
    conn.execute('''INSERT INTO trade_events
        (event_id,trade_id,event_type,event_time,price,size,pnl_delta,fee_amount,
         fee_currency,note,order_id,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(event_id) DO UPDATE SET event_time=excluded.event_time,
        price=excluded.price,size=excluded.size,pnl_delta=excluded.pnl_delta,
        fee_amount=excluded.fee_amount,fee_currency=excluded.fee_currency,
        note=excluded.note,order_id=excluded.order_id''',
        (key,trade_id,kind,event['event_time'],event['price'],event['size'],
         event['pnl_delta'],event['fee_amount'],'USDT',note,event['order_id'],now))
