package mtymes.smartqueue.api;

import mtymes.smartqueue.domain.TaskId;

// todo: mtymes - define the api
public interface SmartQueueClient {

    TaskId submitTask();
}
