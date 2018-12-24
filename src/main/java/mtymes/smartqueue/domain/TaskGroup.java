package mtymes.smartqueue.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import javafixes.object.Microtype;

public class TaskGroup extends Microtype<String> {

    private TaskGroup(String value) {
        super(value);
    }

    @JsonCreator
    public static TaskGroup taskGroup(String value) {
        return new TaskGroup(value);
    }
}
