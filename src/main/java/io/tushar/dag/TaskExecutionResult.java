/*
 * Copyright 2026 Tushar Srivastava.
 * Author: Tushar Srivastava
 * Project: DAG Execution Engine
 */
package io.tushar.dag;
import java.time.Duration;
import java.util.Optional;
public record TaskExecutionResult(String taskId, TaskStatus status, int attempts,
                                  Duration duration, Optional<String> failure) {}
