package mtymes.smartqueue.api;

import mtymes.smartqueue.domain.JobRequestId;

// todo: mtymes - define the api
public interface SmartQueueClient {

    JobRequestId submitJobRequest();
}
