package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TaskConfig extends DataObject {

    public final TaskGroup taskGroup;
    public final int attemptCount;

    public TaskConfig(TaskGroup taskGroup, int attemptCount) {
        checkNotNull(taskGroup, "taskGroup must be defined");
        checkArgument(attemptCount > 0, "attemptCount must be greater than 0");
        this.attemptCount = attemptCount;
        this.taskGroup = taskGroup;
    }
}
