package pool;

import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== Инициализация CustomThreadPool ===");
        CustomThreadPool pool = new CustomThreadPool(
                2,                      // corePoolSize
                4,                      // maxPoolSize
                3,                      // keepAliveTime
                TimeUnit.SECONDS,       // timeUnit
                2,                      // queueSize (per queue, total 4)
                1,                      // minSpareThreads
                new CustomThreadPool.CallerRunsPolicy() // RejectedExecutionHandler
        );

        System.out.println("\n=== Отправка задач ===");
        // Генерируем 12 задач. 
        // 4 задачи займут 4 потока. 
        // 4 задачи лягут в 2 очереди (размер каждой 2).
        // Оставшиеся 4 задачи будут отклонены и выполнены в главном потоке (CallerRunsPolicy).
        for (int i = 1; i <= 12; i++) {
            final int taskId = i;
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(1000); // Имитация долгой работы
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                
                @Override
                public int hashCode() {
                    return taskId; // Для красивого логирования ID задачи
                }
                
                @Override
                public String toString() {
                    return "Task-" + taskId;
                }
            };
            pool.execute(task);
            Thread.sleep(50); // Небольшая задержка, чтобы видеть логи в правильном порядке
        }

        System.out.println("\n=== Ожидание завершения задач и таймаута ===");
        // Ждем, пока потоки обработают задачи и затем завершатся по таймауту (keepAliveTime = 3s)
        Thread.sleep(5000); 

        System.out.println("\n=== Остановка пула ===");
        pool.shutdown();
        
        System.out.println("Главный поток завершен.");
    }
}
