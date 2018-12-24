package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import java.time.ZonedDateTime;

// todo: rename to Execution
public class Run extends DataObject {

    public final TaskId taskId;
    public final TaskGroup taskGroup;
    // todo: mtymes rename to ExecutionId
    public final RunId runId;
    public final ZonedDateTime createdAt;
    public final ZonedDateTime updatedAt;
    public final RunState runState;

    public Run(TaskId taskId, TaskGroup taskGroup, RunId runId, ZonedDateTime createdAt, ZonedDateTime updatedAt, RunState runState) {
        this.taskId = taskId;
        this.taskGroup = taskGroup;
        this.runId = runId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.runState = runState;
    }
}
