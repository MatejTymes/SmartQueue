package mtymes.smartqueue.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import javafixes.object.Microtype;

import java.util.UUID;

public class JobId extends Microtype<UUID> {

    private JobId(UUID value) {
        super(value);
    }

    public static JobId jobId(UUID value) {
        return new JobId(value);
    }

    @JsonCreator
    public static JobId jobId(String value) {
        return new JobId(UUID.fromString(value));
    }
}
