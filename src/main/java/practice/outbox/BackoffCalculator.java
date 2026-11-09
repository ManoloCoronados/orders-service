package practice.outbox;

import java.util.concurrent.ThreadLocalRandom;

final class BackoffCalculator {

    private BackoffCalculator (){}

    static long delayMs(int attempt, long baseMs, long maxMs) {
        int shift = Math.min(Math.max(attempt - 1, 0), 30);
        long exponential = baseMs << shift;
        long capped = Math.min(maxMs, exponential < 0 ? maxMs : exponential);
        long half = capped / 2;
        return half + ThreadLocalRandom.current().nextLong(half + 1);
    }
}
