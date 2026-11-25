"""One public-data worker; all position/state management stays on the caller."""
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Event
import time


class MarketReader:
    """Only public candle and clock methods are exposed to the data pipeline."""
    def __init__(self, client, stopped):
        self.client = client
        self.stopped = stopped

    def fetch_ohlcv(self, *args, **kwargs):
        if self.stopped.is_set():
            raise RuntimeError("market loader closed")
        return self.client.fetch_ohlcv(*args, **kwargs)

    def milliseconds(self):
        return self.client.milliseconds()

    def parse_timeframe(self, tf):
        return self.client.parse_timeframe(tf)


class MarketDataLoader:
    def __init__(self, factory, load_data, poll_interval=5.0):
        if poll_interval <= 0:
            raise ValueError("poll interval must be positive")
        self.factory = factory
        self.load_data = load_data
        self.poll_interval = poll_interval
        self.cache = {}
        self.stopped = Event()
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="bot1-market")
        self.future = None
        self.expired = False

    def _load(self):
        client = self.factory()
        try:
            return self.load_data(client=MarketReader(client, self.stopped), cache=self.cache)
        finally:
            session = getattr(client, "session", None)
            if session is not None:
                session.close()

    def load(self, engine, timeout=60.0):
        if self.stopped.is_set():
            raise RuntimeError("market loader closed")
        if self.future is None:
            self.future = self.executor.submit(self._load)
        deadline = time.monotonic() + timeout
        while True:
            engine.poll()
            if self.future.done():
                future, self.future = self.future, None
                if self.expired:
                    # Retain its cache, not its stale frame bundle; refresh in this call.
                    self.expired = False
                    if time.monotonic() >= deadline:
                        return None
                    self.future = self.executor.submit(self._load)
                    continue
                return future.result()
            left = deadline - time.monotonic()
            if left <= 0:
                self.expired = True
                return None  # Retain the single in-flight job, never queue duplicates.
            wait([self.future], timeout=min(self.poll_interval, left))

    def close(self):
        self.stopped.set()
        self.executor.shutdown(wait=False, cancel_futures=True)
