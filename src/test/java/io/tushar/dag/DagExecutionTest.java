/*
 * Copyright 2026 Tushar Srivastava.
 * Author: Tushar Srivastava
 * Project: DAG Execution Engine
 */
package io.tushar.dag;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class DagExecutionTest {
    @Test void rejectsCyclesAndMissingDependencies() {
        assertThrows(DagValidationException.class, () -> Dag.builder("bad")
            .task("a", c -> {}, "missing").build());
        assertThrows(DagValidationException.class, () -> Dag.builder("cycle")
            .task("a", c -> {}, "b").task("b", c -> {}, "a").build());
    }
    @Test void executesDependenciesAndIndependentBranches() {
        var dag = Dag.builder("ok").task("a", c -> {}).task("b", c -> {}, "a")
            .task("c", c -> {}, "a").build();
        try (var executor = DagExecutor.builder().parallelism(2).build()) {
            var result = executor.execute(dag);
            assertEquals(DagStatus.SUCCEEDED, result.status());
            assertEquals(3, result.tasks().size());
        }
    }

    @Test void supportsTasksDeclaredBeforeTheirDependencies() {
        var dag = Dag.builder("reverse-order")
            .task("publish", c -> {}, "transform")
            .task("transform", c -> {}, "extract")
            .task("extract", c -> {})
            .build();
        try (var executor = DagExecutor.builder().parallelism(2).build()) {
            var result = executor.execute(dag);
            assertEquals(DagStatus.SUCCEEDED, result.status());
            assertEquals(3, result.tasks().size());
        }
    }

    @Test void retriesThenSucceeds() {
        AtomicInteger count = new AtomicInteger();
        var retry = RetryPolicy.builder().maxAttempts(3)
            .initialBackoff(Duration.ZERO).maxBackoff(Duration.ZERO).build();
        var dag = Dag.builder("retry").task("a", c -> {
            if (count.incrementAndGet() < 3) throw new IllegalStateException("transient");
        }, Set.of(), retry).build();
        try (var executor = DagExecutor.builder().parallelism(1).build()) {
            var result = executor.execute(dag);
            assertEquals(TaskStatus.SUCCEEDED, result.tasks().get("a").status());
            assertEquals(3, result.tasks().get("a").attempts());
        }
    }
    @Test void failureSkipsDescendantsButNotIndependentTasks() {
        var dag = Dag.builder("failure").task("bad", c -> { throw new IllegalStateException(); })
            .task("child", c -> fail("must be skipped"), "bad")
            .task("independent", c -> {}).build();
        try (var executor = DagExecutor.builder().parallelism(2).build()) {
            var result = executor.execute(dag);
            assertEquals(DagStatus.FAILED, result.status());
            assertEquals(TaskStatus.SKIPPED, result.tasks().get("child").status());
            assertEquals(TaskStatus.SUCCEEDED, result.tasks().get("independent").status());
        }
    }
}
