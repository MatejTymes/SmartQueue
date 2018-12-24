package mtymes.smartqueue.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import javafixes.object.Microtype;

import java.util.UUID;

// todo: rename to ExecutionId
public class RunId extends Microtype<UUID> {

    private RunId(UUID value) {
        super(value);
    }

    public static RunId runId(UUID value) {
        return new RunId(value);
    }

    @JsonCreator
    public static RunId runId(String value) {
        return new RunId(UUID.fromString(value));
    }
}
