/*
 * Copyright 2026 Tushar Srivastava.
 * Author: Tushar Srivastava
 * Project: DAG Execution Engine
 */
package io.tushar.dag;
import java.util.Objects;
import java.util.Set;
public record DagTask(String id, Set<String> dependencies, TaskAction action, RetryPolicy retryPolicy) {
    public DagTask {
        if (id == null || id.isBlank()) throw new IllegalArgumentException("Task id cannot be blank");
        dependencies = Set.copyOf(Objects.requireNonNull(dependencies));
        Objects.requireNonNull(action); Objects.requireNonNull(retryPolicy);
        if (dependencies.contains(id)) throw new IllegalArgumentException("Task cannot depend on itself: " + id);
    }
}
