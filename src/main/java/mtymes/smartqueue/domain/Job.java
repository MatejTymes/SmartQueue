package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

public class Job extends DataObject {

    public final JobRequestId jobRequestId;
    public final JobId jobId;

    public Job(JobRequestId jobRequestId, JobId jobId) {
        this.jobRequestId = jobRequestId;
        this.jobId = jobId;
    }

    public JobRequestId getJobRequestId() {
        return jobRequestId;
    }

    public JobId getJobId() {
        return jobId;
    }
}
