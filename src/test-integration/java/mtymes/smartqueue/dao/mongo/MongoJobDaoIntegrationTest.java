package mtymes.smartqueue.dao.mongo;

import mtymes.smartqueue.domain.*;
import mtymes.test.db.EmbeddedDB;
import mtymes.test.db.MongoManager;
import mtymes.test.time.FixedClock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static javafixes.common.CollectionUtil.newList;
import static mtymes.smartqueue.dao.mongo.MongoCollections.jobRequestsCollection;
import static mtymes.test.OptionalMatcher.*;
import static mtymes.test.Random.randomJobRequestId;
import static mtymes.test.Random.randomMillis;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MongoJobDaoIntegrationTest {

    private static EmbeddedDB db;
    private static FixedClock clock = new FixedClock();

    private static MongoJobDao jobDao;

    @BeforeClass
    public static void initDB() {
        db = MongoManager.getEmbeddedDB();
        jobDao = new MongoJobDao(jobRequestsCollection(db.getDatabase()), clock);
    }

    @Before
    public void setUp() throws Exception {
        db.removeAllData();
    }

    @AfterClass
    public static void releaseDB() {
        MongoManager.release(db);
    }


    @Test
    public void shouldNotLoadNonExistingJobRequest() {
        assertThat(jobDao.loadJobRequest(randomJobRequestId()), isNotPresent());
    }

    @Test
    public void shouldBeAbleToCreateJobRequest() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());

        // When
        JobRequestId jobRequestId = jobDao.submitJobRequest();

        // Then
        JobRequest expectedJobRequest = new JobRequest(
                jobRequestId,
                submissionTime,
                submissionTime,
                JobRequestState.QUEUED,
                emptyList()
        );
        assertThat(jobDao.loadJobRequest(jobRequestId), isPresentAndEqualTo(expectedJobRequest));
    }

    @Test
    public void shouldNotBeAbleToTakeJobIfNoJobRequestExists() {
        // When
        Optional<Job> job = jobDao.takeNextAvailableJob();

        // Then
        assertThat(job, isNotPresent());
    }

    @Test
    public void shouldBeAbleToTakeJobForSubmittedJobRequest() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        JobRequestId jobRequestId = jobDao.submitJobRequest();

        // When
        ZonedDateTime jobCreationTime = clock.increaseBy(randomMillis());
        Optional<Job> job = jobDao.takeNextAvailableJob();

        // Then
        assertThat(job, isPresent());
        Job expectedJob = new Job(
                jobRequestId,
                job.get().jobId,
                jobCreationTime,
                jobCreationTime,
                JobState.CREATED
        );
        assertThat(job, isPresentAndEqualTo(expectedJob));

        JobRequest expectedJobRequest = new JobRequest(
                jobRequestId,
                submissionTime,
                jobCreationTime,
                JobRequestState.TAKEN,
                newList(expectedJob)
        );
        assertThat(jobDao.loadJobRequest(jobRequestId), isPresentAndEqualTo(expectedJobRequest));
    }

    @Test
    public void shouldBeAbleToMarkJobAsSucceeded() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        JobRequestId jobRequestId = jobDao.submitJobRequest();

        ZonedDateTime jobCreationTime = clock.increaseBy(randomMillis());
        Job job = jobDao.takeNextAvailableJob().get();

        // When
        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = jobDao.markAsSucceeded(job.jobId);

        // Then
        assertThat(wasSuccess, is(true));
        JobRequest expectedJobRequest = new JobRequest(
                jobRequestId,
                submissionTime,
                successTime,
                JobRequestState.SUCCEEDED,
                newList(new Job(
                        jobRequestId,
                        job.jobId,
                        jobCreationTime,
                        successTime,
                        JobState.SUCCEEDED
                ))
        );
        assertThat(jobDao.loadJobRequest(jobRequestId), isPresentAndEqualTo(expectedJobRequest));
    }

    @Test
    public void shouldNotBeAbleToMarkJobAsSucceededTwice() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        JobRequestId jobRequestId = jobDao.submitJobRequest();

        ZonedDateTime jobCreationTime = clock.increaseBy(randomMillis());
        Job job = jobDao.takeNextAvailableJob().get();

        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        jobDao.markAsSucceeded(job.jobId);

        // When
        ZonedDateTime secondSuccessTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = jobDao.markAsSucceeded(job.jobId);

        // Then
        assertThat(wasSuccess, is(false));
        JobRequest expectedJobRequest = new JobRequest(
                jobRequestId,
                submissionTime,
                successTime,
                JobRequestState.SUCCEEDED,
                newList(new Job(
                        jobRequestId,
                        job.jobId,
                        jobCreationTime,
                        successTime,
                        JobState.SUCCEEDED
                ))
        );
        assertThat(jobDao.loadJobRequest(jobRequestId), isPresentAndEqualTo(expectedJobRequest));
    }

    @Test
    public void shouldBeAbleToMarkJobAsFailed() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        JobRequestId jobRequestId = jobDao.submitJobRequest();

        ZonedDateTime jobCreationTime = clock.increaseBy(randomMillis());
        Job job = jobDao.takeNextAvailableJob().get();

        // When
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = jobDao.markAsFailed(job.jobId);

        // Then
        assertThat(wasSuccess, is(true));
        JobRequest expectedJobRequest = new JobRequest(
                jobRequestId,
                submissionTime,
                failureTime,
                JobRequestState.FAILED,
                newList(new Job(
                        jobRequestId,
                        job.jobId,
                        jobCreationTime,
                        failureTime,
                        JobState.FAILED
                ))
        );
        assertThat(jobDao.loadJobRequest(jobRequestId), isPresentAndEqualTo(expectedJobRequest));
    }

    @Test
    public void shouldNotBeAbleToMarkJobAsFailedTwice() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        JobRequestId jobRequestId = jobDao.submitJobRequest();

        ZonedDateTime jobCreationTime = clock.increaseBy(randomMillis());
        Job job = jobDao.takeNextAvailableJob().get();

        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        jobDao.markAsFailed(job.jobId);

        // When
        ZonedDateTime secondFailureTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = jobDao.markAsFailed(job.jobId);

        // Then
        assertThat(wasSuccess, is(false));
        JobRequest expectedJobRequest = new JobRequest(
                jobRequestId,
                submissionTime,
                failureTime,
                JobRequestState.FAILED,
                newList(new Job(
                        jobRequestId,
                        job.jobId,
                        jobCreationTime,
                        failureTime,
                        JobState.FAILED
                ))
        );
        assertThat(jobDao.loadJobRequest(jobRequestId), isPresentAndEqualTo(expectedJobRequest));
    }

    @Test
    public void shouldNotFailSucceededJob() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        JobRequestId jobRequestId = jobDao.submitJobRequest();

        ZonedDateTime jobCreationTime = clock.increaseBy(randomMillis());
        Job job = jobDao.takeNextAvailableJob().get();

        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        jobDao.markAsSucceeded(job.jobId);

        // When
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = jobDao.markAsFailed(job.jobId);

        // Then
        assertThat(wasSuccess, is(false));
        JobRequest expectedJobRequest = new JobRequest(
                jobRequestId,
                submissionTime,
                successTime,
                JobRequestState.SUCCEEDED,
                newList(new Job(
                        jobRequestId,
                        job.jobId,
                        jobCreationTime,
                        successTime,
                        JobState.SUCCEEDED
                ))
        );
        assertThat(jobDao.loadJobRequest(jobRequestId), isPresentAndEqualTo(expectedJobRequest));
    }

    @Test
    public void shouldNotSucceededFailedJob() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        JobRequestId jobRequestId = jobDao.submitJobRequest();

        ZonedDateTime jobCreationTime = clock.increaseBy(randomMillis());
        Job job = jobDao.takeNextAvailableJob().get();

        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        jobDao.markAsFailed(job.jobId);

        // When
        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = jobDao.markAsSucceeded(job.jobId);

        // Then
        assertThat(wasSuccess, is(false));
        JobRequest expectedJobRequest = new JobRequest(
                jobRequestId,
                submissionTime,
                failureTime,
                JobRequestState.FAILED,
                newList(new Job(
                        jobRequestId,
                        job.jobId,
                        jobCreationTime,
                        failureTime,
                        JobState.FAILED
                ))
        );
        assertThat(jobDao.loadJobRequest(jobRequestId), isPresentAndEqualTo(expectedJobRequest));
    }
}