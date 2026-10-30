/*
 * Copyright 2026 Tushar Srivastava.
 * Author: Tushar Srivastava
 * Project: DAG Execution Engine
 */
package io.tushar.dag;
import java.time.Duration;
import java.util.Map;
public record DagExecutionResult(String dagId, DagStatus status,
                                 Map<String, TaskExecutionResult> tasks, Duration duration) {
    public DagExecutionResult { tasks = Map.copyOf(tasks); }
}
