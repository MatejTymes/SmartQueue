package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import java.time.ZonedDateTime;

public class Execution extends DataObject {

    public final TaskId taskId;
    public final TaskGroup taskGroup;
    public final ExecutionId executionId;
    public final ZonedDateTime createdAt;
    public final ZonedDateTime updatedAt;
    public final ExecutionState state;

    public Execution(TaskId taskId, TaskGroup taskGroup, ExecutionId executionId, ZonedDateTime createdAt, ZonedDateTime updatedAt, ExecutionState state) {
        this.taskId = taskId;
        this.taskGroup = taskGroup;
        this.executionId = executionId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.state = state;
    }
}
