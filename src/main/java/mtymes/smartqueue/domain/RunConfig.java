package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RunConfig extends DataObject {

    public final RunGroup runGroup;
    public final int attemptCount;

    public RunConfig(RunGroup runGroup, int attemptCount) {
        checkNotNull(runGroup, "runGroup must be defined");
        checkArgument(attemptCount > 0, "attemptCount must be greater than 0");
        this.attemptCount = attemptCount;
        this.runGroup = runGroup;
    }
}
