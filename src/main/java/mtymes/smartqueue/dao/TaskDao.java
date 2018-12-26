package mtymes.smartqueue.dao;

import mtymes.smartqueue.domain.*;

import java.util.Optional;

public interface TaskDao {

    TaskId submitTask(TaskConfig taskConfig);

    boolean cancelTask(TaskId taskId);

    Optional<Task> loadTask(TaskId taskId);

    Optional<Execution> createNextExecution();

    boolean markAsSucceeded(ExecutionId executionId);

    boolean markAsFailed(ExecutionId executionId);
}
