package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import java.time.ZonedDateTime;
import java.util.List;

public class Task extends DataObject {

    public final TaskId taskId;
    public final TaskGroup taskGroup;
    public final ZonedDateTime submittedAt;
    public final ZonedDateTime updatedAt;
    public final TaskState state;
    public final List<Execution> executions;

    public Task(TaskId taskId, TaskGroup taskGroup, ZonedDateTime submittedAt, ZonedDateTime updatedAt, TaskState state, List<Execution> executions) {
        this.taskId = taskId;
        this.taskGroup = taskGroup;
        this.submittedAt = submittedAt;
        this.updatedAt = updatedAt;
        this.state = state;
        this.executions = executions;
    }
}
