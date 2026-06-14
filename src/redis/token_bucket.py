"""
redis_token_bucket.py
---------------------
Shared rate limiter using Redis.

WHY REDIS?
  Each Lambda is a separate process on a separate machine.
  If we stored the token count inside Lambda memory, every
  Lambda would have its own counter — they'd all think they
  have 10 tokens, and together send 50 requests to an API
  that only allows 10/min.

  Redis is ONE shared counter that ALL Lambda instances read
  and write to. It's the single source of truth.

HOW TOKEN BUCKET WORKS (plain English):
  1. Redis holds a number (e.g. 10) for each API.
  2. Before every HTTP call, Lambda asks Redis: "give me 1 token"
  3. Redis decrements the number (10 → 9 → 8 ... → 0)
  4. When the number hits 0, Lambda WAITS — no HTTP call is made
  5. Every 60 seconds, Redis resets the number back to 10 (refill)
  6. Lambda tries again → gets a token → makes the call
"""

import time
import random
import redis
import logging

logger = logging.getLogger(__name__)


class TokenBucket:
    """
    One TokenBucket instance per API.

    Example:
        bucket = TokenBucket(api_name="api1_fx_rates", rpm=10)
        bucket.acquire()  # blocks until a token is available
        response = requests.get(url)  # only called after token acquired
    """

    def __init__(self, api_name: str, rpm: int, redis_client: redis.Redis):
        self.api_name = api_name
        self.rpm = rpm                          # max requests per minute
        self.redis = redis_client
        self.key = f"token_bucket:{api_name}"  # Redis key for this API

    def _try_acquire(self) -> bool:
        """
        Try to take one token from the bucket.
        Returns True if successful, False if bucket is empty.

        This uses a Redis pipeline to make the check + decrement
        atomic — meaning no two Lambdas can grab the same token
        at the same time.
        """
        with self.redis.pipeline() as pipe:
            try:
                pipe.watch(self.key)

                current = pipe.get(self.key)

                if current is None:
                    # First call ever for this API — fill the bucket
                    pipe.multi()
                    pipe.set(self.key, self.rpm - 1, ex=60)  # TTL = 60s
                    pipe.execute()
                    logger.debug(f"{self.api_name}: bucket initialized with {self.rpm} tokens")
                    return True

                tokens_left = int(current)

                if tokens_left <= 0:
                    # Bucket empty — caller must wait
                    return False

                # Take one token
                pipe.multi()
                pipe.decr(self.key)
                pipe.execute()
                logger.debug(f"{self.api_name}: token acquired, {tokens_left - 1} remaining")
                return True

            except redis.WatchError:
                # Another Lambda modified the key between our watch and execute
                # This is fine — just retry
                return False

    def acquire(self, max_wait_seconds: int = 120):
        """
        Block until a token is available.
        Keeps retrying with small sleeps until it gets one.

        Plain English: "Wait by the ticket booth until a ticket
        becomes available, then take one and proceed."
        """
        waited = 0
        attempt = 0

        while waited < max_wait_seconds:
            if self._try_acquire():
                return  # Got a token — caller can proceed

            # No token available — calculate how long to wait
            # We check Redis TTL to know when the bucket refills
            ttl = self.redis.ttl(self.key)
            if ttl <= 0:
                ttl = 1  # bucket expired, will refill on next check

            # Small jitter so not all Lambdas wake up at the exact same second
            # Without jitter: all 50 Lambdas sleep 5s, then all try at once → problem
            # With jitter: each Lambda wakes at a slightly different time → spread out
            jitter = random.uniform(0, 0.5)
            sleep_time = min(ttl, 2) + jitter

            logger.info(
                f"{self.api_name}: bucket empty, waiting {sleep_time:.1f}s "
                f"(attempt {attempt + 1}, ttl={ttl}s)"
            )
            time.sleep(sleep_time)
            waited += sleep_time
            attempt += 1

        raise TimeoutError(
            f"{self.api_name}: could not acquire token after {max_wait_seconds}s"
        )


class RateLimiterRegistry:
    """
    Holds one TokenBucket per API.
    Lambda imports this once at cold start, then reuses it.

    Usage:
        registry = RateLimiterRegistry(redis_client)
        registry.acquire("api1_fx_rates", rpm=10)
        # now safe to make HTTP call
    """

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self._buckets: dict[str, TokenBucket] = {}

    def acquire(self, api_name: str, rpm: int):
        """Get a token for the given API. Blocks if bucket is empty."""
        if api_name not in self._buckets:
            self._buckets[api_name] = TokenBucket(api_name, rpm, self.redis)
        self._buckets[api_name].acquire()


# ── Quick demo (run this file directly to see it in action) ──────────────────

if __name__ == "__main__":
    """
    Demo: simulate 3 workers all trying to call API1 (limit: 5 rpm)
    Watch how they wait once tokens run out.
    """
    import threading

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    # Connect to Redis (needs Redis running locally: docker run -p 6379:6379 redis)
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    registry = RateLimiterRegistry(r)

    def simulate_worker(worker_id: str, pages: int):
        for page in range(1, pages + 1):
            print(f"  {worker_id}: waiting for token (page {page})...")
            registry.acquire("api1_fx_rates", rpm=5)
            print(f"  {worker_id}: ✓ got token, calling API page {page}")
            time.sleep(0.1)  # simulate HTTP call

    threads = [
        threading.Thread(target=simulate_worker, args=(f"Lambda-{c}", 3))
        for c in ["A", "B", "C"]
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print("\nAll workers done. Zero 429s.")
