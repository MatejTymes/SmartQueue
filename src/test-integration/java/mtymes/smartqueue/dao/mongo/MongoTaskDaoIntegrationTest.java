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
import static mtymes.smartqueue.dao.mongo.MongoCollections.tasksCollection;
import static mtymes.test.OptionalMatcher.*;
import static mtymes.test.Random.randomTaskId;
import static mtymes.test.Random.randomMillis;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

// todo: concurrency tests : combination of success / failure, cancel / running
// todo: extract TaskDaoIntegrationTest - unrelated on mongo
public class MongoTaskDaoIntegrationTest {

    private static EmbeddedDB db;
    private static FixedClock clock = new FixedClock();

    private static MongoTaskDao taskDao;

    @BeforeClass
    public static void initDB() {
        db = MongoManager.getEmbeddedDB();
        taskDao = new MongoTaskDao(tasksCollection(db.getDatabase()), clock);
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
    public void shouldNotLoadNonExistingTask() {
        assertThat(taskDao.loadTask(randomTaskId()), isNotPresent());
    }

    @Test
    public void shouldSubmitTask() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());

        // When
        TaskId taskId = taskDao.submitTask();

        // Then
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                submissionTime,
                TaskState.QUEUED,
                emptyList()
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    /* ==================== */
    /* --- creating run --- */
    /* ==================== */

    @Test
    public void shouldNotCreateRunIfNoTaskExists() {
        // When
        Optional<Run> run = taskDao.createNextAvailableRun();

        // Then
        assertThat(run, isNotPresent());
    }

    @Test
    public void shouldNotCreateRunIfTaskIsCancelled() {
        TaskId taskId = taskDao.submitTask();
        taskDao.cancelTask(taskId);

        // When
        Optional<Run> run = taskDao.createNextAvailableRun();

        // Then
        assertThat(run, isNotPresent());
    }

    @Test
    public void shouldCreateRunForSubmittedTask() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskId taskId = taskDao.submitTask();

        // When
        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        Optional<Run> run = taskDao.createNextAvailableRun();

        // Then
        assertThat(run, isPresent());
        Run expectedRun = new Run(
                taskId,
                run.get().runId,
                runCreationTime,
                runCreationTime,
                RunState.CREATED
        );
        assertThat(run, isPresentAndEqualTo(expectedRun));

        Task expectedTask = new Task(
                taskId,
                submissionTime,
                runCreationTime,
                TaskState.RUNNING,
                newList(expectedRun)
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    /* ==================== */
    /* --- cancellation --- */
    /* ==================== */

    @Test
    public void shouldCancelSubmittedTask() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskId taskId = taskDao.submitTask();

        // When
        ZonedDateTime cancellationTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.cancelTask(taskId);

        // Then
        assertThat(wasSuccess, is(true));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                cancellationTime,
                TaskState.CANCELLED,
                emptyList()
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotCancelSuccessfulTask() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskId taskId = taskDao.submitTask();
        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        RunId runId = taskDao.createNextAvailableRun().get().runId;
        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        taskDao.markAsSucceeded(runId);

        // When
        ZonedDateTime cancellationTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.cancelTask(taskId);

        // Then
        assertThat(wasSuccess, is(false));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                successTime,
                TaskState.SUCCEEDED,
                newList(new Run(
                        taskId,
                        runId,
                        runCreationTime,
                        successTime,
                        RunState.SUCCEEDED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotCancelFailedTask() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskId taskId = taskDao.submitTask();
        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        RunId runId = taskDao.createNextAvailableRun().get().runId;
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        taskDao.markAsFailed(runId);

        // When
        ZonedDateTime cancellationTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.cancelTask(taskId);

        // Then
        assertThat(wasSuccess, is(false));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                failureTime,
                TaskState.FAILED,
                newList(new Run(
                        taskId,
                        runId,
                        runCreationTime,
                        failureTime,
                        RunState.FAILED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    /* =============== */
    /* --- success --- */
    /* =============== */

    @Test
    public void shouldMarkRunAsSucceeded() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskId taskId = taskDao.submitTask();

        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        Run run = taskDao.createNextAvailableRun().get();

        // When
        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsSucceeded(run.runId);

        // Then
        assertThat(wasSuccess, is(true));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                successTime,
                TaskState.SUCCEEDED,
                newList(new Run(
                        taskId,
                        run.runId,
                        runCreationTime,
                        successTime,
                        RunState.SUCCEEDED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotMarkRunAsSucceededTwice() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskId taskId = taskDao.submitTask();

        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        Run run = taskDao.createNextAvailableRun().get();

        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        taskDao.markAsSucceeded(run.runId);

        // When
        ZonedDateTime secondSuccessTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsSucceeded(run.runId);

        // Then
        assertThat(wasSuccess, is(false));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                successTime,
                TaskState.SUCCEEDED,
                newList(new Run(
                        taskId,
                        run.runId,
                        runCreationTime,
                        successTime,
                        RunState.SUCCEEDED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotSucceededFailedRun() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskId taskId = taskDao.submitTask();

        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        Run run = taskDao.createNextAvailableRun().get();

        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        taskDao.markAsFailed(run.runId);

        // When
        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsSucceeded(run.runId);

        // Then
        assertThat(wasSuccess, is(false));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                failureTime,
                TaskState.FAILED,
                newList(new Run(
                        taskId,
                        run.runId,
                        runCreationTime,
                        failureTime,
                        RunState.FAILED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    /* =============== */
    /* --- failure --- */
    /* =============== */

    @Test
    public void shouldMarkRunAsFailed() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskId taskId = taskDao.submitTask();

        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        Run run = taskDao.createNextAvailableRun().get();

        // When
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsFailed(run.runId);

        // Then
        assertThat(wasSuccess, is(true));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                failureTime,
                TaskState.FAILED,
                newList(new Run(
                        taskId,
                        run.runId,
                        runCreationTime,
                        failureTime,
                        RunState.FAILED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotMarkRunAsFailedTwice() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskId taskId = taskDao.submitTask();

        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        Run run = taskDao.createNextAvailableRun().get();

        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        taskDao.markAsFailed(run.runId);

        // When
        ZonedDateTime secondFailureTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsFailed(run.runId);

        // Then
        assertThat(wasSuccess, is(false));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                failureTime,
                TaskState.FAILED,
                newList(new Run(
                        taskId,
                        run.runId,
                        runCreationTime,
                        failureTime,
                        RunState.FAILED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotFailSucceededRun() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskId taskId = taskDao.submitTask();

        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        Run run = taskDao.createNextAvailableRun().get();

        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        taskDao.markAsSucceeded(run.runId);

        // When
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsFailed(run.runId);

        // Then
        assertThat(wasSuccess, is(false));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                successTime,
                TaskState.SUCCEEDED,
                newList(new Run(
                        taskId,
                        run.runId,
                        runCreationTime,
                        successTime,
                        RunState.SUCCEEDED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }
}