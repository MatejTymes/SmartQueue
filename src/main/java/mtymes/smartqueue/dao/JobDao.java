package mtymes.smartqueue.dao;

import mtymes.smartqueue.domain.Job;
import mtymes.smartqueue.domain.JobId;
import mtymes.smartqueue.domain.JobRequestId;

import java.util.Optional;

public interface JobDao {

    JobRequestId submitJobRequest();

    Optional<Job> takeNextAvailableJob();

    boolean markAsSucceeded(JobId jobId);

    boolean markAsFailed(JobId jobId);
}