package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import java.time.ZonedDateTime;
import java.util.List;

public class Task extends DataObject {

    public final TaskId taskId;
    public final ZonedDateTime submittedAt;
    public final ZonedDateTime updatedAt;
    public final TaskState state;
    public final List<Run> runs;

    public Task(TaskId taskId, ZonedDateTime submittedAt, ZonedDateTime updatedAt, TaskState state, List<Run> runs) {
        this.taskId = taskId;
        this.submittedAt = submittedAt;
        this.updatedAt = updatedAt;
        this.state = state;
        this.runs = runs;
    }
}
