package mtymes.smartqueue.dao;

import mtymes.smartqueue.domain.*;

import java.util.Optional;

public interface TaskDao {

    TaskId submitTask(TaskConfig config, TaskBody body);

    Optional<Task> loadTask(TaskId taskId);

    Optional<TaskBody> loadTaskBody(TaskId taskId);

    // todo: provide Optional<ExecutionId> lastAssumedExecutionId
    boolean cancelTask(TaskId taskId);

    Optional<Execution> createNextExecution();

    boolean markAsSucceeded(ExecutionId executionId);

    boolean markAsFailed(ExecutionId executionId);
}
