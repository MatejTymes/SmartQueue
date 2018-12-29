package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import static com.google.common.base.Preconditions.checkArgument;

public class TaskConfig extends DataObject {

    public final int attemptCount;

    public TaskConfig(int attemptCount) {
        checkArgument(attemptCount > 0, "attemptCount must be greater than 0");
        this.attemptCount = attemptCount;
    }
}
