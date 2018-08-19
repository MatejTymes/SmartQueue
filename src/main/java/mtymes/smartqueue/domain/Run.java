package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import java.time.ZonedDateTime;

public class Run extends DataObject {

    // todo: mtymes - maybe remove taskId
    public final TaskId taskId;
    public final RunId runId;
    public final ZonedDateTime createdAt;
    public final ZonedDateTime updatedAt;
    public final RunState runState;

    public Run(TaskId taskId, RunId runId, ZonedDateTime createdAt, ZonedDateTime updatedAt, RunState runState) {
        this.taskId = taskId;
        this.runId = runId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.runState = runState;
    }
}
