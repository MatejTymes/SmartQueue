package mtymes.smartqueue.dao.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import mtymes.common.mongo.DocWrapper;
import mtymes.common.time.Clock;
import mtymes.smartqueue.dao.JobDao;
import mtymes.smartqueue.domain.*;
import org.apache.commons.lang3.NotImplementedException;
import org.bson.Document;

import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.Optional;

import static com.mongodb.client.model.ReturnDocument.AFTER;
import static java.util.UUID.randomUUID;
import static mtymes.common.mongo.DocBuilder.doc;
import static mtymes.common.mongo.DocBuilder.docBuilder;
import static mtymes.common.mongo.DocWrapper.wrap;
import static mtymes.smartqueue.domain.JobId.jobId;
import static mtymes.smartqueue.domain.JobRequestId.jobRequestId;

public class MongoJobDao implements JobDao {

    private static final String JOB_REQUEST_ID = "_id";
    private static final String CREATED_AT_TIME = "createdAt";
    private static final String UPDATED_AT_TIME = "updatedAt";
    protected static final String STATE = "state";

    private static final String JOBS = "jobs";
    private static final String JOB_ID = "jobId";

    private final MongoCollection<Document> jobRequests;

    private final Clock clock;

    public MongoJobDao(MongoCollection<Document> jobRequests, Clock clock) {
        this.jobRequests = jobRequests;
        this.clock = clock;
    }

    @Override
    public Optional<JobRequest> loadJobRequest(JobRequestId jobRequestId) {
        return one(jobRequests.find(doc(JOB_REQUEST_ID, jobRequestId))).map(this::toJobRequest);
    }

    @Override
    public JobRequestId submitJobRequest() {
        JobRequestId jobRequestId = jobRequestId(randomUUID());

        ZonedDateTime now = clock.now();
        jobRequests.insertOne(docBuilder()
                .put(JOB_REQUEST_ID, jobRequestId)
                .put(CREATED_AT_TIME, now)
                .put(UPDATED_AT_TIME, now)
                .put(STATE, JobRequestState.QUEUED)
                .build());

        return jobRequestId;
    }

    @Override
    public Optional<Job> takeNextAvailableJob() {
        ZonedDateTime now = clock.now();

        JobId jobId = jobId(randomUUID());
        Document document = jobRequests.findOneAndUpdate(
                doc(STATE, JobRequestState.QUEUED),
                docBuilder()
                        .put("$addToSet", doc(JOBS, docBuilder()
                                .put(JOB_ID, jobId)
                                .put(CREATED_AT_TIME, now)
                                .put(UPDATED_AT_TIME, now)
                                .put(STATE, JobState.CREATED)
                                .build()))
                        .put("$set", docBuilder()
                                .put(STATE, JobRequestState.TAKEN)
                                .put(UPDATED_AT_TIME, now)
                                .build())
                        .build(),
                // todo: test this
                sortBy(doc(UPDATED_AT_TIME, 1))
        );

        return Optional.ofNullable(document).map(doc -> {
            DocWrapper dbJobRequest = wrap(doc);
            JobRequestId jobRequestId = dbJobRequest.getJobRequestId(JOB_REQUEST_ID);
            DocWrapper dbJob = dbJobRequest.getList(JOBS).lastDoc();
            return toJob(
                    jobRequestId,
                    dbJob
            );
        });
    }

    // todo: test this - can't do it twice, can't change failed one
    @Override
    public boolean markAsSucceeded(JobId jobId) {
        ZonedDateTime now = clock.now();

        long modifiedCount = jobRequests.updateOne(
                docBuilder()
                        .put(STATE, JobRequestState.TAKEN)
                        .put(JOBS, doc("$elemMatch", docBuilder()
                                .put(JOB_ID, jobId)
                                .put(STATE, JobState.CREATED)
                                .build()))
                        .build(),
                docBuilder()
                        .put("$set", docBuilder()
                                .put(STATE, JobRequestState.SUCCEEDED)
                                .put(UPDATED_AT_TIME, now)
                                .put(JOBS + ".$." + STATE, JobState.SUCCEEDED)
                                .put(JOBS + ".$." + UPDATED_AT_TIME, now)
                                .build())
                        .build()
        ).getModifiedCount();

        return modifiedCount == 1;
    }

    // todo: test this - can't do it twice, can't change succeeded one
    @Override
    public boolean markAsFailed(JobId jobId) {
        ZonedDateTime now = clock.now();

        long modifiedCount = jobRequests.updateOne(
                docBuilder()
                        .put(STATE, JobRequestState.TAKEN)
                        .put(JOBS, doc("$elemMatch", docBuilder()
                                .put(JOB_ID, jobId)
                                .put(STATE, JobState.CREATED)
                                .build()))
                        .build(),
                docBuilder()
                        .put("$set", docBuilder()
                                .put(STATE, JobRequestState.FAILED)
                                .put(UPDATED_AT_TIME, now)
                                .put(JOBS + ".$." + STATE, JobState.FAILED)
                                .put(JOBS + ".$." + UPDATED_AT_TIME, now)
                                .build())
                        .build()
        ).getModifiedCount();

        return modifiedCount == 1;
    }

    private JobRequest toJobRequest(Document doc) {
        DocWrapper dbJobRequest = wrap(doc);

        JobRequestId jobRequestId = dbJobRequest.getJobRequestId(JOB_REQUEST_ID);

        return new JobRequest(
                jobRequestId,
                dbJobRequest.getZonedDateTime(CREATED_AT_TIME),
                dbJobRequest.getZonedDateTime(UPDATED_AT_TIME),
                dbJobRequest.getJobRequestState(STATE),
                dbJobRequest.getList(JOBS, true).mapDoc(dbJob -> toJob(jobRequestId, dbJob))
        );
    }

    private Job toJob(JobRequestId jobRequestId, DocWrapper dbJob) {
        return new Job(
                jobRequestId,
                dbJob.getJobId(JOB_ID),
                dbJob.getZonedDateTime(CREATED_AT_TIME),
                dbJob.getZonedDateTime(UPDATED_AT_TIME),
                dbJob.getJobState(STATE)
        );
    }

    private FindOneAndUpdateOptions sortBy(Document sortBy) {
        return new FindOneAndUpdateOptions()
                .sort(sortBy)
                .returnDocument(AFTER);
    }

    private <T> Optional<T> one(Iterable<T> items) {
        Iterator<T> iterator = items.iterator();
        if (iterator.hasNext()) {
            T value = iterator.next();
            if (iterator.hasNext()) {
                throw new IllegalStateException("unexpected second item");
            }
            return Optional.ofNullable(value);
        } else {
            return Optional.empty();
        }
    }
}
