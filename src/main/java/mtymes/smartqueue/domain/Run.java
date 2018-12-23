package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import java.time.ZonedDateTime;

// todo: mtymes - rename to TaskRun
public class Run extends DataObject {

    // todo: mtymes - maybe remove taskId
    public final TaskId taskId;
    // todo: mtymes - maybe remove runGroup and keep just the taskId
    // todo: mtymes - test this value
    public final RunGroup runGroup;
    // todo: mtymes rename to TaskRunId
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
