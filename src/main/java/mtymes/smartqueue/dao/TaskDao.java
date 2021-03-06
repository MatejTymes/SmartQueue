package mtymes.smartqueue.dao;

import mtymes.smartqueue.domain.*;

import java.time.Duration;
import java.util.Optional;

public interface TaskDao {

    TaskId submitTask(TaskConfig config, TaskBody body);

    Optional<Task> loadTask(TaskId taskId);

    Optional<TaskBody> loadTaskBody(TaskId taskId);

    boolean cancelTask(TaskId taskId, Optional<ExecutionId> lastAssumedExecutionId);

    Optional<Execution> createNextExecution();

    boolean markAsSucceeded(ExecutionId executionId);

    boolean markAsFailed(ExecutionId executionId);

    boolean setTTL(TaskId taskId, Duration duration);

    boolean keepForever(TaskId taskId);

    Optional<Duration> getTTL(TaskId taskId);
}
