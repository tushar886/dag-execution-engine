/*
 * Copyright 2026 Tushar Srivastava.
 * Author: Tushar Srivastava
 * Project: DAG Execution Engine
 */
package io.tushar.dag;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DagExecutor implements AutoCloseable {
    private final ExecutorService workers;
    private final AtomicBoolean closed = new AtomicBoolean();

    private DagExecutor(int parallelism, ThreadFactory factory) {
        if (parallelism < 1) throw new IllegalArgumentException("parallelism must be >= 1");
        workers = Executors.newFixedThreadPool(parallelism, factory);
    }
    public static Builder builder() { return new Builder(); }

    public DagExecutionResult execute(Dag dag) {
        Objects.requireNonNull(dag);
        if (closed.get()) throw new IllegalStateException("Executor is closed");
        Instant started = Instant.now();
        Map<String, CompletableFuture<TaskExecutionResult>> futures = new LinkedHashMap<>();
        for (DagTask task : dag.tasks().values()) {
            CompletableFuture<?>[] dependencies = task.dependencies().stream()
                .map(futures::get).toArray(CompletableFuture[]::new);
            CompletableFuture<TaskExecutionResult> future = CompletableFuture
                .allOf(dependencies)
                .thenApplyAsync(ignored -> {
                    boolean dependenciesSucceeded = task.dependencies().stream()
                        .map(futures::get)
                        .allMatch(dependency -> dependency.join().status() == TaskStatus.SUCCEEDED);
                    return dependenciesSucceeded
                        ? executeTask(dag.id(), task)
                        : skipped(task.id());
                }, workers);
            futures.put(task.id(), future);
        }
        CompletableFuture.allOf(futures.values().toArray(CompletableFuture[]::new)).join();
        Map<String, TaskExecutionResult> results = new LinkedHashMap<>();
        futures.forEach((id, future) -> results.put(id, future.join()));
        DagStatus status = results.values().stream().anyMatch(r -> r.status() == TaskStatus.FAILED)
            ? DagStatus.FAILED : DagStatus.SUCCEEDED;
        return new DagExecutionResult(dag.id(), status, results, Duration.between(started, Instant.now()));
    }

    private TaskExecutionResult executeTask(String dagId, DagTask task) {
        Instant started = Instant.now();
        int attempts = 0;
        Throwable last = null;
        while (attempts < task.retryPolicy().maxAttempts()) {
            attempts++;
            try {
                task.action().execute(new TaskContext(dagId, task.id(), attempts, Instant.now(), Map.of()));
                return new TaskExecutionResult(task.id(), TaskStatus.SUCCEEDED, attempts,
                    Duration.between(started, Instant.now()), Optional.empty());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); last = e; break;
            } catch (Exception e) {
                last = e;
                if (attempts >= task.retryPolicy().maxAttempts()) break;
                try {
                    TimeUnit.MILLISECONDS.sleep(task.retryPolicy().delayBeforeAttempt(attempts + 1).toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt(); last = interrupted; break;
                }
            }
        }
        String detail = last == null ? "Unknown failure" :
            last.getClass().getSimpleName() + ": " + String.valueOf(last.getMessage());
        return new TaskExecutionResult(task.id(), TaskStatus.FAILED, attempts,
            Duration.between(started, Instant.now()), Optional.of(detail));
    }
    private static TaskExecutionResult skipped(String id) {
        return new TaskExecutionResult(id, TaskStatus.SKIPPED, 0, Duration.ZERO,
            Optional.of("A dependency did not succeed"));
    }
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        workers.shutdown();
        try {
            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) workers.shutdownNow();
        } catch (InterruptedException e) {
            workers.shutdownNow(); Thread.currentThread().interrupt();
        }
    }
    public static final class Builder {
        private int parallelism = Runtime.getRuntime().availableProcessors();
        private ThreadFactory factory = r -> new Thread(r, "dag-worker");
        public Builder parallelism(int value) { parallelism = value; return this; }
        public Builder threadFactory(ThreadFactory value) { factory = Objects.requireNonNull(value); return this; }
        public DagExecutor build() { return new DagExecutor(parallelism, factory); }
    }
}
