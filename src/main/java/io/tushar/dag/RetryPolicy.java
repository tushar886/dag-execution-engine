/*
 * Copyright 2026 Tushar Srivastava.
 * Author: Tushar Srivastava
 * Project: DAG Execution Engine
 */
package io.tushar.dag;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class RetryPolicy {
    private final int maxAttempts;
    private final Duration initialBackoff, maxBackoff;
    private final double jitter;

    private RetryPolicy(int maxAttempts, Duration initial, Duration max, double jitter) {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
        if (initial.isNegative() || max.isNegative() || max.compareTo(initial) < 0)
            throw new IllegalArgumentException("Invalid backoff durations");
        if (jitter < 0 || jitter > 1) throw new IllegalArgumentException("jitter must be in [0,1]");
        this.maxAttempts = maxAttempts; this.initialBackoff = initial;
        this.maxBackoff = max; this.jitter = jitter;
    }
    public static RetryPolicy noRetries() {
        return new RetryPolicy(1, Duration.ZERO, Duration.ZERO, 0);
    }
    public static Builder builder() { return new Builder(); }
    public int maxAttempts() { return maxAttempts; }

    Duration delayBeforeAttempt(int nextAttempt) {
        if (nextAttempt <= 1 || initialBackoff.isZero()) return Duration.ZERO;
        long initial = initialBackoff.toMillis(), cap = maxBackoff.toMillis();
        int shift = Math.min(nextAttempt - 2, 30);
        long exponential;
        try { exponential = Math.multiplyExact(initial, 1L << shift); }
        catch (ArithmeticException overflow) { exponential = Long.MAX_VALUE; }
        long base = Math.min(exponential, cap);
        double factor = 1 - jitter + ThreadLocalRandom.current().nextDouble() * 2 * jitter;
        return Duration.ofMillis(Math.max(0, Math.round(base * factor)));
    }
    public static final class Builder {
        private int attempts = 1;
        private Duration initial = Duration.ZERO, max = Duration.ZERO;
        private double jitter;
        public Builder maxAttempts(int v) { attempts = v; return this; }
        public Builder initialBackoff(Duration v) { initial = Objects.requireNonNull(v); return this; }
        public Builder maxBackoff(Duration v) { max = Objects.requireNonNull(v); return this; }
        public Builder jitter(double v) { jitter = v; return this; }
        public RetryPolicy build() { return new RetryPolicy(attempts, initial, max, jitter); }
    }
}
