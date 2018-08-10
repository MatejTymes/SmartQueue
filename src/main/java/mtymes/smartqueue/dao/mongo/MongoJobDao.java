package mtymes.smartqueue.dao.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import mtymes.common.time.Clock;
import mtymes.smartqueue.dao.JobDao;
import mtymes.smartqueue.domain.Job;
import mtymes.smartqueue.domain.JobId;
import mtymes.smartqueue.domain.JobRequestId;
import org.apache.commons.lang3.NotImplementedException;
import org.bson.Document;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import static com.mongodb.client.model.ReturnDocument.AFTER;
import static java.util.UUID.randomUUID;
import static mtymes.common.mongo.DocBuilder.doc;
import static mtymes.common.mongo.DocBuilder.docBuilder;
import static mtymes.smartqueue.domain.JobId.jobId;
import static mtymes.smartqueue.domain.JobRequestId.jobRequestId;

public class MongoJobDao implements JobDao {

    private static final String JOB_REQUEST_ID = "_id";
    private static final String INSERTED_AT_TIME = "insertedAt";
    private static final String UPDATED_AT_TIME = "updatedAt";
    protected static final String STATUS = "status";

    private static final String JOBS = "jobs";
    private static final String JOB_ID = "jobId";


    private static final String QUEUED_STATUS = "queued";
    private static final String TAKEN_STATUS = "taken";
//    private static final String STARTED_STATUS = "started";


    private final MongoCollection<Document> jobRequests;

    private final Clock clock;

    public MongoJobDao(MongoCollection<Document> jobRequests, Clock clock) {
        this.jobRequests = jobRequests;
        this.clock = clock;
    }

    @Override
    public JobRequestId submitJobRequest() {
        JobRequestId jobRequestId = jobRequestId(randomUUID());

        ZonedDateTime now = clock.now();
        jobRequests.insertOne(docBuilder()
                .put(JOB_REQUEST_ID, jobRequestId)
                .put(INSERTED_AT_TIME, now)
                .put(UPDATED_AT_TIME, now)
                .put(STATUS, QUEUED_STATUS)
                .build());

        return jobRequestId;
    }

    @Override
    public Optional<Job> takeNextAvailableJob() {
        ZonedDateTime now = clock.now();

        JobId jobId = jobId(randomUUID());
        Document document = jobRequests.findOneAndUpdate(
                doc(STATUS, QUEUED_STATUS),
                docBuilder()
                        .put("$addToSet", doc(JOBS, doc(JOB_ID, jobId)))
                        .put("$set", docBuilder()
                                .put(STATUS, TAKEN_STATUS)
                                .put(UPDATED_AT_TIME, now)
                                .build())
                        .build(),
                // todo: test this
                sortBy(doc(UPDATED_AT_TIME, 1))
        );

        return Optional.ofNullable(document).map(this::toJob);
    }

    // todo: test this
    @Override
    public boolean markAsSucceeded(JobId jobId) {
        // todo: implement
//        ZonedDateTime now = clock.now();
//
//        long modifiedCount = jobRequests.updateOne(
//                docBuilder()
//                        .put(JOB_ID, jobId)
//                        .put(STATUS, TAKEN_STATUS)
//                        .build(),
//                docBuilder()
//                        .put(STATUS, STARTED_STATUS)
//                        .put(UPDATED_AT_TIME, now)
//                        .build()
//        ).getModifiedCount();
//
//        return modifiedCount == 1;
        throw new NotImplementedException("implement me");
    }

    // todo: test this
    @Override
    public boolean markAsFailed(JobId jobId) {
        // todo: implement
        throw new NotImplementedException("implement me");
    }

    private Job toJob(Document doc) {
        // todo: mtymes - use DocWrapper instead
        List<Document> list = (List<Document>) doc.get(JOBS);
        return new Job(
                jobRequestId(doc.getString(JOB_REQUEST_ID)),
                jobId(list.get(list.size() - 1).getString(JOB_ID))
        );
    }

    private FindOneAndUpdateOptions sortBy(Document sortBy) {
        return new FindOneAndUpdateOptions()
                .sort(sortBy)
                .returnDocument(AFTER);
    }
}
