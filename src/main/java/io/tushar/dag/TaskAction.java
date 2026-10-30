/*
 * Copyright 2026 Tushar Srivastava.
 * Author: Tushar Srivastava
 * Project: DAG Execution Engine
 */
package io.tushar.dag;
@FunctionalInterface
public interface TaskAction {
    void execute(TaskContext context) throws Exception;
}
