package mtymes.smartqueue.dao;

import mtymes.smartqueue.domain.*;

import java.util.Optional;

public interface TaskDao {

    TaskId submitTask(RunConfig runConfig);

    boolean cancelTask(TaskId taskId);

    Optional<Task> loadTask(TaskId taskId);

    Optional<Run> createNextAvailableRun();

    boolean markAsSucceeded(RunId runId);

    boolean markAsFailed(RunId runId);
}
