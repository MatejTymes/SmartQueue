package mtymes.test;

import mtymes.smartqueue.domain.JobId;
import mtymes.smartqueue.domain.JobRequestId;

import java.util.UUID;

import static mtymes.smartqueue.domain.JobId.jobId;
import static mtymes.smartqueue.domain.JobRequestId.jobRequestId;

public class Random {

    public static UUID randomUUID() {
        return UUID.randomUUID();
    }

    public static JobRequestId randomJobRequestId() {
        return jobRequestId(randomUUID());
    }

    public static JobId randomJobId() {
        return jobId(randomUUID());
    }
}
