package mtymes.smartqueue.dao;

import mtymes.smartqueue.domain.*;
import mtymes.smartqueue.domain.query.ExecutionQuery;

import java.util.Optional;

import static mtymes.smartqueue.domain.query.ExecutionQuery.emptyQuery;

public interface TaskDao {

    TaskId submitTask(TaskConfig taskConfig);

    boolean cancelTask(TaskId taskId);

    Optional<Task> loadTask(TaskId taskId);

    Optional<Execution> createNextExecution(ExecutionQuery query);

    default Optional<Execution> createNextExecution() {
        return createNextExecution(emptyQuery());
    }

    boolean markAsSucceeded(ExecutionId executionId);

    boolean markAsFailed(ExecutionId executionId);
}
