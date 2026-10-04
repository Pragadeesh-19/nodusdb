package io.nodusdb.lake;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;

final class ManualExecutor implements Executor {

    private final Queue<Runnable> tasks = new ArrayDeque<>();

    @Override
    public synchronized void execute(Runnable task) {
        tasks.add(task);
    }

    synchronized int pending() {
        return tasks.size();
    }

    void runAll() {
        Runnable task;
        while ((task = nextTask()) != null) {
            task.run();
        }
    }

    private synchronized Runnable nextTask() {
        return tasks.poll();
    }
}
