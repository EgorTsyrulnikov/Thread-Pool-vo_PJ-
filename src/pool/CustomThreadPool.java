package pool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadPool implements CustomExecutor {
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTimeNanos;
    private final int queueSize;
    private final int minSpareThreads;
    private final RejectedExecutionHandler rejectedExecutionHandler;
    private final ThreadFactory threadFactory;

    private final BlockingQueue<Runnable>[] queues;
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    private final List<Worker> workers = new ArrayList<>();
    private final AtomicInteger activeThreads = new AtomicInteger(0);

    private volatile boolean isShutdown = false;
    private volatile boolean isShutdownNow = false;

    @SuppressWarnings("unchecked")
    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit timeUnit,
                            int queueSize, int minSpareThreads, RejectedExecutionHandler rejectedExecutionHandler) {
        if (corePoolSize <= 0 || maxPoolSize < corePoolSize || keepAliveTime < 0 || queueSize <= 0 || minSpareThreads < 0) {
            throw new IllegalArgumentException("Invalid thread pool parameters");
        }
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTimeNanos = timeUnit.toNanos(keepAliveTime);
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.rejectedExecutionHandler = rejectedExecutionHandler != null ? rejectedExecutionHandler : new CallerRunsPolicy();
        this.threadFactory = new CustomThreadFactory("CustomPool");

        this.queues = new BlockingQueue[corePoolSize];
        for (int i = 0; i < corePoolSize; i++) {
            this.queues[i] = new ArrayBlockingQueue<>(queueSize);
        }
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) throw new NullPointerException();
        if (isShutdown) {
            System.out.println("[Rejected] Task " + command.hashCode() + " was rejected due to pool being shut down.");
            rejectedExecutionHandler.rejectedExecution(command, this);
            return;
        }

        ensureCoreAndSpareThreads();

        // Попытка добавить задачу в одну из очередей по принципу Round Robin
        int attempts = 0;
        boolean added = false;
        while (attempts < queues.length) {
            int qIndex = (roundRobinCounter.getAndIncrement() & Integer.MAX_VALUE) % queues.length;
            if (queues[qIndex].offer(command)) {
                System.out.println("[Pool] Task accepted into queue #" + qIndex + " (id " + command.hashCode() + ")");
                added = true;
                break;
            }
            attempts++;
        }

        if (!added) {
            // Очереди переполнены. Если возможно, создаем новый поток сверх corePoolSize
            synchronized (workers) {
                if (workers.size() < maxPoolSize) {
                    Worker worker = new Worker(null, command); // Передаем задачу напрямую новому воркеру
                    workers.add(worker);
                    worker.startWorker();
                    System.out.println("[Pool] Queues full, started new thread up to maxPoolSize for task " + command.hashCode());
                    return; // Успешно передано
                }
            }
            // Если мы дошли сюда, то очереди полны и потоков уже maxPoolSize
            System.out.println("[Rejected] Task " + command.hashCode() + " was rejected due to overload!");
            rejectedExecutionHandler.rejectedExecution(command, this);
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) throw new NullPointerException();
        FutureTask<T> task = new FutureTask<>(callable);
        execute(task);
        return task;
    }

    private void ensureCoreAndSpareThreads() {
        synchronized (workers) {
            int total = workers.size();
            int active = activeThreads.get();
            int idle = total - active;

            // Если потоков меньше базового, просто создаем новый
            if (total < corePoolSize) {
                Worker worker = new Worker(queues[total % queues.length], null);
                workers.add(worker);
                worker.startWorker();
            } 
            // Иначе проверяем резервные потоки (minSpareThreads)
            else if (idle < minSpareThreads && total < maxPoolSize) {
                Worker worker = new Worker(queues[total % queues.length], null);
                workers.add(worker);
                worker.startWorker();
                System.out.println("[Pool] Created new spare thread due to low idle count. Total: " + workers.size());
            }
        }
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        System.out.println("[Pool] Shutdown initiated.");
        synchronized (workers) {
            for (Worker w : workers) {
                w.interruptIfIdle();
            }
        }
    }

    @Override
    public void shutdownNow() {
        isShutdown = true;
        isShutdownNow = true;
        System.out.println("[Pool] ShutdownNow initiated.");
        for (BlockingQueue<Runnable> queue : queues) {
            queue.clear();
        }
        synchronized (workers) {
            for (Worker w : workers) {
                w.interruptWorker();
            }
        }
    }

    public boolean isShutdownNow() {
        return isShutdownNow;
    }
    
    public boolean isShutdown() {
        return isShutdown;
    }

    private boolean removeWorker(Worker worker) {
        synchronized (workers) {
            if (workers.size() > corePoolSize) {
                workers.remove(worker);
                return true;
            }
            return false;
        }
    }

    private class Worker implements Runnable {
        private final BlockingQueue<Runnable> queue;
        private Runnable firstTask;
        private Thread thread;
        private volatile boolean isWorking = false;

        public Worker(BlockingQueue<Runnable> queue, Runnable firstTask) {
            if (queue == null) {
                int index = (int)(Math.random() * queues.length);
                this.queue = queues[index];
            } else {
                this.queue = queue;
            }
            this.firstTask = firstTask;
        }

        public void startWorker() {
            this.thread = threadFactory.newThread(this);
            this.thread.start();
        }

        public void interruptIfIdle() {
            if (!isWorking && thread != null) {
                thread.interrupt();
            }
        }

        public void interruptWorker() {
            if (thread != null) {
                thread.interrupt();
            }
        }

        @Override
        public void run() {
            Runnable task = firstTask;
            firstTask = null;

            while (!isShutdownNow) {
                if (task == null) {
                    try {
                        task = queue.poll(keepAliveTimeNanos, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        if (isShutdown && queue.isEmpty()) break;
                        continue;
                    }
                }

                if (task == null) {
                    // Таймаут по keepAliveTime
                    if (removeWorker(this)) {
                        System.out.println("[Worker] " + thread.getName() + " idle timeout, stopping.");
                        break; // Завершаем работу потока
                    } else {
                        continue; // Не можем завершить, так как мы core поток
                    }
                }

                if (isShutdownNow) break;

                // Выполнение задачи
                isWorking = true;
                activeThreads.incrementAndGet();
                System.out.println("[Worker] " + thread.getName() + " executes task " + task.hashCode());
                
                try {
                    task.run();
                } catch (RuntimeException e) {
                    System.err.println("[Worker] Error executing task: " + e.getMessage());
                } finally {
                    activeThreads.decrementAndGet();
                    isWorking = false;
                    task = null;
                }
                
                // Если был shutdown, но не shutdownNow, и очереди пустые - выходим
                if (isShutdown) {
                    boolean empty = true;
                    for (BlockingQueue<Runnable> q : queues) {
                        if (!q.isEmpty()) {
                            empty = false;
                            break;
                        }
                    }
                    if (empty) break;
                }
            }
            System.out.println("[Worker] " + thread.getName() + " terminated.");
        }
    }

    public static class CallerRunsPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, CustomThreadPool executor) {
            if (!executor.isShutdown()) {
                System.out.println("[CallerRunsPolicy] Executing task in caller thread: " + Thread.currentThread().getName());
                r.run();
            }
        }
    }
}
