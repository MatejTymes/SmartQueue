package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import static com.google.common.base.Preconditions.checkArgument;

public class RunConfig extends DataObject {

    public final int attemptCount;

    public RunConfig(int attemptCount) {
        checkArgument(attemptCount > 0, "attemptCount must be greater than 0");
        this.attemptCount = attemptCount;
    }
}
