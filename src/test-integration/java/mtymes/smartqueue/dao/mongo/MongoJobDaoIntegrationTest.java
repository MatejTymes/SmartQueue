package mtymes.smartqueue.dao.mongo;

import mtymes.common.time.Clock;
import mtymes.smartqueue.dao.TestIdGenerator;
import mtymes.smartqueue.domain.Job;
import mtymes.smartqueue.domain.JobId;
import mtymes.smartqueue.domain.JobRequestId;
import mtymes.test.OptionalMatcher;
import mtymes.test.db.EmbeddedDB;
import mtymes.test.db.MongoManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Optional;

import static mtymes.smartqueue.dao.mongo.MongoCollections.jobRequestsCollection;
import static mtymes.test.OptionalMatcher.isNotPresent;
import static mtymes.test.OptionalMatcher.isPresent;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class MongoJobDaoIntegrationTest {

    private static EmbeddedDB db;
    private static Clock clock = new Clock(); // todo: maybe change to use StubClock

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
    public void shouldNotBeAbleToTakeNextJobIfNoJobRequestExists() {
        // When
        Optional<Job> job = jobDao.takeNextAvailableJob();

        // Then
        assertThat(job, isNotPresent());
    }

    @Test
    public void shouldBeAbleToTakeJobForSubmittedJobRequest() {
        // When
        JobRequestId jobRequestId = jobDao.submitJobRequest();
        Optional<Job> job = jobDao.takeNextAvailableJob();

        // Then
        assertThat(job, isPresent());
        assertThat(job.get(), equalTo(new Job(jobRequestId, job.get().jobId)));
    }
}