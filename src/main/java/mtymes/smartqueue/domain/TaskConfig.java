package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import java.time.Duration;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class TaskConfig extends DataObject {

    public final int attemptCount;
    public final Optional<Duration> ttl;

    public TaskConfig(int attemptCount, Optional<Duration> ttl) {
        checkArgument(attemptCount > 0, "attemptCount must be greater than 0");
        checkArgument(ttl != null, "ttl can't be null");
        if (ttl.isPresent()) {
            checkArgument(ttl.get().toMillis() >= 0, "ttl can't have negative value");
        }

        this.attemptCount = attemptCount;
        this.ttl = ttl;
    }
}
