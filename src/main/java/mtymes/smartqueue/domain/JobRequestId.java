package mtymes.smartqueue.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import javafixes.object.Microtype;

import java.util.UUID;

public class JobRequestId extends Microtype<UUID> {

    private JobRequestId(UUID value) {
        super(value);
    }

    public static JobRequestId jobRequestId(UUID value) {
        return new JobRequestId(value);
    }

    @JsonCreator
    public static JobRequestId jobRequestId(String value) {
        return new JobRequestId(UUID.fromString(value));
    }
}
