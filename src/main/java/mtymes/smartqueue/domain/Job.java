package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import java.time.ZonedDateTime;

public class Job extends DataObject {

    // todo: mtymes - maybe remove jobRequestId
    public final JobRequestId jobRequestId;
    public final JobId jobId;
    public final ZonedDateTime createdAt;
    public final ZonedDateTime updatedAt;
    public final JobState jobState;

    public Job(JobRequestId jobRequestId, JobId jobId, ZonedDateTime createdAt, ZonedDateTime updatedAt, JobState jobState) {
        this.jobRequestId = jobRequestId;
        this.jobId = jobId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.jobState = jobState;
    }
}
