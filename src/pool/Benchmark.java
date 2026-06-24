package pool;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Benchmark {
    private static final int TASK_COUNT = 100_000;
    private static final int PRODUCER_THREADS = 4;
    private static final int CORE_POOL_SIZE = 4;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Начинаем реалистичный бенчмарк (100,000 задач)...");
        System.out.println("Логи пула будут отключены на время теста, чтобы не искажать результаты метрик из-за вывода в консоль.\n");

        java.io.PrintStream originalOut = System.out;
        java.io.PrintStream nullOut = new java.io.PrintStream(new java.io.OutputStream() {
            public void write(int b) {}
        });

        System.setOut(nullOut); // Отключаем логи

        // 1. Стандартный ThreadPoolExecutor
        ThreadPoolExecutor standardPool = new ThreadPoolExecutor(
                CORE_POOL_SIZE, CORE_POOL_SIZE,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()
        );
        long stdTime = runBenchmark(standardPool);
        standardPool.shutdown();

        // 2. Наш CustomThreadPool
        CustomThreadPool customPool = new CustomThreadPool(
                CORE_POOL_SIZE, CORE_POOL_SIZE,
                1000, TimeUnit.MILLISECONDS,
                TASK_COUNT, 0, new CustomThreadPool.CallerRunsPolicy()
        );
        long customTime = runBenchmark(customPool);
        customPool.shutdown();

        System.setOut(originalOut); // Возвращаем логи

        System.out.println("=== ИТОГИ БЕНЧМАРКА ===");
        System.out.println("Standard Pool : " + stdTime + " ms");
        System.out.println("Custom Pool   : " + customTime + " ms");
        
        if (customTime < stdTime) {
            System.out.println("Наш пул быстрее на " + (stdTime - customTime) + " ms!");
        } else {
            System.out.println("Наш пул медленнее на " + (customTime - stdTime) + " ms. Это ожидаемо при малом contention и накладных расходах RoundRobin.");
        }
    }

    private static long runBenchmark(Executor pool) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(TASK_COUNT);
        
        Runnable dummyTask = () -> {
            // Имитируем легкую полезную нагрузку
            double res = 0;
            for (int i = 0; i < 50; i++) {
                res += Math.sin(i);
            }
            latch.countDown();
        };

        ExecutorService producers = Executors.newFixedThreadPool(PRODUCER_THREADS);
        
        long startTime = System.currentTimeMillis();
        
        for (int p = 0; p < PRODUCER_THREADS; p++) {
            producers.execute(() -> {
                for (int i = 0; i < TASK_COUNT / PRODUCER_THREADS; i++) {
                    pool.execute(dummyTask);
                }
            });
        }
        
        latch.await(); // Ждем выполнения всех 100 000 задач
        long endTime = System.currentTimeMillis();
        
        producers.shutdown();
        long duration = endTime - startTime;
        System.out.println("Завершено за: " + duration + " ms");
        return duration;
    }
}
