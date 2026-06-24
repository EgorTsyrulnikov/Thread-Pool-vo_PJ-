package pool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadFactory implements ThreadFactory {
    private final String namePrefix;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    public CustomThreadFactory(String poolName) {
        this.namePrefix = poolName + "-worker-";
    }

    @Override
    public Thread newThread(Runnable r) {
        String name = namePrefix + threadNumber.getAndIncrement();
        Thread t = new Thread(r, name);
        System.out.println("[ThreadFactory] Creating new thread: " + name);
        return t;
    }
}
