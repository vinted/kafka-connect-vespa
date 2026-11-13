package com.vinted.kafka.connect.vespa.mocks;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Runs feed client answers on its own threads once the thread that built it waits for them, as the real client does. */
public final class CallbackExecutor implements Executor, AutoCloseable {
    public static final String THREAD_NAME = "feed-client-callback";
    private static final long PATIENCE_NANOS = TimeUnit.SECONDS.toNanos(5);

    private final Thread taskThread = Thread.currentThread();
    private final ExecutorService threads;

    public CallbackExecutor(int threads) {
        this.threads = Executors.newFixedThreadPool(threads, runnable -> new Thread(runnable, THREAD_NAME));
    }

    @Override
    public void execute(Runnable answer) {
        threads.execute(() -> {
            awaitTaskThreadWaiting();
            answer.run();
        });
    }

    @Override
    public void close() throws InterruptedException {
        threads.shutdown();
        threads.awaitTermination(10, TimeUnit.SECONDS);
    }

    // Answering earlier could complete a future before the task attaches its callback, which then runs on the task thread.
    private void awaitTaskThreadWaiting() {
        long deadline = System.nanoTime() + PATIENCE_NANOS;
        while (taskThread.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }
}
