/*
 * Copyright 2026 Tushar Srivastava.
 * Author: Tushar Srivastava
 * Project: DAG Execution Engine
 */
package io.tushar.dag;
import java.util.*;

public final class Dag {
    private final String id;
    private final Map<String, DagTask> tasks;
    private Dag(String id, Collection<DagTask> input) {
        if (id == null || id.isBlank()) throw new DagValidationException("DAG id cannot be blank");
        this.id = id;
        LinkedHashMap<String, DagTask> indexed = new LinkedHashMap<>();
        for (DagTask task : input)
            if (indexed.putIfAbsent(task.id(), task) != null)
                throw new DagValidationException("Duplicate task id: " + task.id());
        if (indexed.isEmpty()) throw new DagValidationException("DAG must contain at least one task");
        for (DagTask task : indexed.values())
            for (String dep : task.dependencies())
                if (!indexed.containsKey(dep))
                    throw new DagValidationException("Unknown dependency '" + dep + "' for task '" + task.id() + "'");
        tasks = Collections.unmodifiableMap(topologicalOrder(indexed));
    }
    public static Builder builder(String id) { return new Builder(id); }
    public String id() { return id; }
    public Map<String, DagTask> tasks() { return tasks; }

    private static LinkedHashMap<String, DagTask> topologicalOrder(Map<String, DagTask> tasks) {
        Map<String, Integer> degree = new LinkedHashMap<>();
        Map<String, List<String>> children = new LinkedHashMap<>();
        tasks.values().forEach(t -> {
            degree.put(t.id(), t.dependencies().size());
            t.dependencies().forEach(d -> children.computeIfAbsent(d, k -> new ArrayList<>()).add(t.id()));
        });
        Deque<String> ready = new ArrayDeque<>();
        degree.forEach((k, v) -> { if (v == 0) ready.add(k); });
        int visited = 0;
        LinkedHashMap<String, DagTask> ordered = new LinkedHashMap<>();
        while (!ready.isEmpty()) {
            String id = ready.remove(); visited++;
            ordered.put(id, tasks.get(id));
            for (String child : children.getOrDefault(id, List.of()))
                if (degree.compute(child, (k, v) -> v - 1) == 0) ready.add(child);
        }
        if (visited != tasks.size()) throw new DagValidationException("DAG contains a cycle");
        return ordered;
    }

    public static final class Builder {
        private final String id;
        private final List<DagTask> tasks = new ArrayList<>();
        private Builder(String id) { this.id = id; }
        public Builder task(String id, TaskAction action, String... dependencies) {
            return task(id, action, Set.of(dependencies), RetryPolicy.noRetries());
        }
        public Builder task(String id, TaskAction action, Set<String> dependencies, RetryPolicy policy) {
            tasks.add(new DagTask(id, dependencies, action, policy)); return this;
        }
        public Dag build() { return new Dag(id, tasks); }
    }
}
