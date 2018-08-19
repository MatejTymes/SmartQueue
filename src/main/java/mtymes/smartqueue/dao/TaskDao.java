package mtymes.smartqueue.dao;

import mtymes.smartqueue.domain.Run;
import mtymes.smartqueue.domain.RunId;
import mtymes.smartqueue.domain.Task;
import mtymes.smartqueue.domain.TaskId;

import java.util.Optional;

public interface TaskDao {

    TaskId submitTask();

    boolean cancelTask(TaskId taskId);

    Optional<Task> loadTask(TaskId taskId);

    Optional<Run> createNextAvailableRun();

    boolean markAsSucceeded(RunId runId);

    boolean markAsFailed(RunId runId);
}
