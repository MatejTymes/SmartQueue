package mtymes.smartqueue.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import javafixes.object.Microtype;

import java.util.UUID;

public class ExecutionId extends Microtype<UUID> {

    private ExecutionId(UUID value) {
        super(value);
    }

    public static ExecutionId executionId(UUID value) {
        return new ExecutionId(value);
    }

    @JsonCreator
    public static ExecutionId executionId(String value) {
        return new ExecutionId(UUID.fromString(value));
    }
}
