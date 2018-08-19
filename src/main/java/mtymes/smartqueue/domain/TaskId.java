package mtymes.smartqueue.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import javafixes.object.Microtype;

import java.util.UUID;

public class TaskId extends Microtype<UUID> {

    private TaskId(UUID value) {
        super(value);
    }

    public static TaskId taskId(UUID value) {
        return new TaskId(value);
    }

    @JsonCreator
    public static TaskId taskId(String value) {
        return new TaskId(UUID.fromString(value));
    }
}
