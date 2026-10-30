/*
 * Copyright 2026 Tushar Srivastava.
 * Author: Tushar Srivastava
 * Project: DAG Execution Engine
 */
package io.tushar.dag;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
public record TaskContext(String dagId, String taskId, int attempt,
                          Instant startedAt, Map<String, Object> attributes) {
    public TaskContext {
        Objects.requireNonNull(dagId); Objects.requireNonNull(taskId);
        Objects.requireNonNull(startedAt);
        attributes = Map.copyOf(Objects.requireNonNull(attributes));
        if (attempt < 1) throw new IllegalArgumentException("attempt must be >= 1");
    }
}
