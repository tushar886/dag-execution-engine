# DAG Execution Engine

**Author:** Tushar Srivastava  
**Language:** Java 21  
**Build:** Maven 3.9+

A small Java library for validating and executing dependency graphs inside a single JVM. It is intended for applications that need bounded parallel execution of independent work without introducing a workflow platform or an external scheduler.

## What it does

- Validates task identifiers, dependency references, and cycles before execution.
- Resolves dependencies and schedules independent tasks on a bounded worker pool.
- Retries failed task actions using capped exponential backoff and optional jitter.
- Marks descendants of unsuccessful tasks as `SKIPPED`; unrelated branches continue.
- Returns an immutable, task-level execution summary, including attempts and elapsed time.

## Example

```java
var dag = Dag.builder("daily-ingestion")
    .task("extract", context -> extract())
    .task("validate", context -> validate(), "extract")
    .task("publish", context -> publish(), "validate")
    .task("audit", context -> audit(), "extract")
    .build();

try (var executor = DagExecutor.builder().parallelism(4).build()) {
    DagExecutionResult result = executor.execute(dag);
    System.out.println(result.status());
}
```

## Retry configuration

`maxAttempts` includes the initial invocation. For example, `maxAttempts(3)` permits one initial attempt and up to two retries.

```java
var retry = RetryPolicy.builder()
    .maxAttempts(3)
    .initialBackoff(Duration.ofMillis(100))
    .maxBackoff(Duration.ofSeconds(2))
    .jitter(0.20)
    .build();
```

Pass the policy to a task with the overload that accepts dependencies and a `RetryPolicy`.

## Execution semantics

1. The graph is validated and topologically ordered when `build()` is called. Invalid graphs fail fast with `DagValidationException`.
2. A task becomes eligible only after all of its dependencies have completed.
3. A task action runs only when all dependencies have `SUCCEEDED` status.
4. If a task exhausts its attempts, it is `FAILED`; its descendants are `SKIPPED`. Independent branches are not cancelled.
5. The DAG status is `FAILED` if at least one task failed. A DAG with skipped tasks but no failed task is not produced by the current dependency rules.

## Design boundaries

This is deliberately an in-process execution component, not a durable workflow engine. It does not provide persistence, distributed coordination, task timeouts, cancellation, dynamic graph mutation, or recovery after process termination. The configured parallelism bounds task actions running through one executor instance.

Retry delays currently occupy a worker thread. Keep retry delays short; a separate timer scheduler would be preferable for workloads with long backoff periods. Task actions that perform external side effects should be idempotent because a failed attempt may have partially completed its work before the retry.

The executor owns its worker pool and should be closed when no longer needed. Use one executor per intended concurrency boundary rather than creating one per task.

## Build and test

Requires JDK 21 or newer and Maven 3.9 or newer.

```bash
mvn clean verify
```

The test suite covers graph validation, dependency ordering, retry behavior, and failure propagation.

