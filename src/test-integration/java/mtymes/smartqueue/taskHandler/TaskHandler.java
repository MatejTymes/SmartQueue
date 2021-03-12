package mtymes.smartqueue.taskHandler;

import mtymes.smartqueue.domain.*;

import java.time.Duration;
import java.util.Optional;

public interface TaskHandler {

    TaskId submitTask();

    TaskId submitTask(TaskConfig taskConfig);

    boolean doesTaskExist(TaskId taskId);

    boolean cancelTask(TaskId taskId);

    boolean cancelTask(TaskId taskId, ExecutionId executionId);

    Optional<Execution> createNextExecution();

    boolean markAsSucceeded(ExecutionId executionId);

    boolean markAsFailed(ExecutionId executionId);

    TaskState loadTaskState(TaskId taskId);

    ExecutionState loadExecutionState(ExecutionId executionId);

    ExecutionId loadLastExecutionId(TaskId taskId);

    boolean setTtl(TaskId taskId, Duration ttl);

    void waitFor(Duration duration);
}
