package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

import java.time.ZonedDateTime;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static javafixes.common.CollectionUtil.newList;

public class JobRequest extends DataObject {

    public final JobRequestId jobRequestId;
    public final ZonedDateTime createdAt;
    public final ZonedDateTime updatedAt;
    public final JobRequestState state;
    public final List<Job> jobs;

    public JobRequest(JobRequestId jobRequestId, ZonedDateTime createdAt, ZonedDateTime updatedAt, JobRequestState state, List<Job> jobs) {
        this.jobRequestId = jobRequestId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.state = state;
        this.jobs = jobs;
    }
}
