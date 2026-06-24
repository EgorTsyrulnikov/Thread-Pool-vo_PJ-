package pool;

public interface RejectedExecutionHandler {
    void rejectedExecution(Runnable r, CustomThreadPool executor);
}
