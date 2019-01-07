package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

public class Task extends DataObject {

    public final TaskId taskId;
    public final ZonedDateTime submittedAt;
    public final ZonedDateTime updatedAt;
    public final TaskState state;
    public final Optional<ExecutionId> lastExecutionId;
    public final List<Execution> executions; // todo: make this an optional feature

    public Task(
            TaskId taskId,
            ZonedDateTime submittedAt,
            ZonedDateTime updatedAt,
            TaskState state,
            Optional<ExecutionId> lastExecutionId,
            List<Execution> executions
    ) {
        this.taskId = taskId;
        this.submittedAt = submittedAt;
        this.updatedAt = updatedAt;
        this.state = state;
        this.lastExecutionId = lastExecutionId;
        this.executions = executions;
    }
}
