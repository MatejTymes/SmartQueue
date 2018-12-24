package mtymes.smartqueue.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import javafixes.object.Microtype;

import java.util.UUID;

// todo: rename to TaskGroup
public class RunGroup extends Microtype<String> {

    private RunGroup(String value) {
        super(value);
    }

    @JsonCreator
    public static RunGroup runGroup(String value) {
        return new RunGroup(value);
    }
}
