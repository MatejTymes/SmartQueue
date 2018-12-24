package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import java.time.ZonedDateTime;

// todo: rename to Execution
public class Run extends DataObject {

    public final TaskId taskId;
    public final RunGroup runGroup;
    // todo: mtymes rename to ExecutionId
    public final RunId runId;
    public final ZonedDateTime createdAt;
    public final ZonedDateTime updatedAt;
    public final RunState runState;

    public Run(TaskId taskId, RunGroup runGroup, RunId runId, ZonedDateTime createdAt, ZonedDateTime updatedAt, RunState runState) {
        this.taskId = taskId;
        this.runGroup = runGroup;
        this.runId = runId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.runState = runState;
    }
}
