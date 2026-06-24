package pool;

import java.util.concurrent.*;

public class Benchmark {
    private static final int WARMUP_COUNT = 20_000;
    private static final int TASK_COUNT = 100_000;
    private static final int PRODUCER_THREADS = 4;
    private static final int CORE_POOL_SIZE = 4;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Начинаем реалистичный бенчмарк (Warmup: " + WARMUP_COUNT + ", Задач: " + TASK_COUNT + ")...");
        System.out.println("Логи пула будут отключены на время теста, чтобы не искажать результаты метрик из-за вывода в консоль.\n");

        java.io.PrintStream originalOut = System.out;
        java.io.PrintStream nullOut = new java.io.PrintStream(new java.io.OutputStream() {
            public void write(int b) {}
        });

        // Прогрев JVM (Warmup)
        System.out.println("Выполняется прогрев JIT-компилятора...");
        ThreadPoolExecutor warmupPool = new ThreadPoolExecutor(
                CORE_POOL_SIZE, CORE_POOL_SIZE,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()
        );
        System.setOut(nullOut);
        runBenchmarkTasks(warmupPool, WARMUP_COUNT);
        warmupPool.shutdown();
        System.setOut(originalOut);
        System.out.println("Прогрев завершен.\n");

        System.setOut(nullOut); // Отключаем логи для основного теста

        // 1. Стандартный ThreadPoolExecutor
        ThreadPoolExecutor standardPool = new ThreadPoolExecutor(
                CORE_POOL_SIZE, CORE_POOL_SIZE,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()
        );
        long stdTime = runBenchmarkTasks(standardPool, TASK_COUNT);
        standardPool.shutdown();

        // 2. Наш CustomThreadPool
        CustomThreadPool customPool = new CustomThreadPool(
                CORE_POOL_SIZE, CORE_POOL_SIZE,
                1000, TimeUnit.MILLISECONDS,
                TASK_COUNT, 0, new CustomThreadPool.CallerRunsPolicy()
        );
        long customTime = runBenchmarkTasks(customPool, TASK_COUNT);
        customPool.shutdown();

        System.setOut(originalOut); // Возвращаем логи

        System.out.println("=== ИТОГИ БЕНЧМАРКА ===");
        System.out.println("Standard Pool : " + stdTime + " ms");
        System.out.println("Custom Pool   : " + customTime + " ms");
        
        if (customTime < stdTime) {
            System.out.println("Наш пул быстрее на " + (stdTime - customTime) + " ms!");
        } else {
            System.out.println("Наш пул медленнее на " + (customTime - stdTime) + " ms. Это ожидаемо при глобальной синхронизации и линейном обходе Least Loaded.");
        }
    }

    private static long runBenchmarkTasks(Executor pool, int tasks) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(tasks);
        
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
                for (int i = 0; i < tasks / PRODUCER_THREADS; i++) {
                    pool.execute(dummyTask);
                }
            });
        }
        
        latch.await(); // Ждем выполнения задач
        long endTime = System.currentTimeMillis();
        
        producers.shutdown();
        return endTime - startTime;
    }
}
