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
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static javafixes.common.CollectionUtil.newList;
import static mtymes.smartqueue.dao.mongo.MongoCollections.tasksCollection;
import static mtymes.test.OptionalMatcher.*;
import static mtymes.test.Random.*;
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
        TaskConfig taskConfig = commonTaskConfig();

        // When
        TaskId taskId = taskDao.submitTask(taskConfig);

        // Then
        Task expectedTask = new Task(
                taskId,
                taskConfig.taskGroup,
                submissionTime,
                submissionTime,
                TaskState.SUBMITTED,
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
        TaskId taskId = taskDao.submitTask(commonTaskConfig());
        taskDao.cancelTask(taskId);

        // When
        Optional<Run> run = taskDao.createNextAvailableRun();

        // Then
        assertThat(run, isNotPresent());
    }

    @Test
    public void shouldCreateRunForSubmittedTask() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

        // When
        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        Optional<Run> run = taskDao.createNextAvailableRun();

        // Then
        assertThat(run, isPresent());
        Run expectedRun = new Run(
                taskId,
                taskConfig.taskGroup,
                run.get().runId,
                runCreationTime,
                runCreationTime,
                RunState.CREATED
        );
        assertThat(run, isPresentAndEqualTo(expectedRun));

        Task expectedTask = new Task(
                taskId,
                taskConfig.taskGroup,
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
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

        // When
        ZonedDateTime cancellationTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.cancelTask(taskId);

        // Then
        assertThat(wasSuccess, is(true));
        Task expectedTask = new Task(
                taskId,
                taskConfig.taskGroup,
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
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);
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
                taskConfig.taskGroup,
                submissionTime,
                successTime,
                TaskState.SUCCEEDED,
                newList(new Run(
                        taskId,
                        taskConfig.taskGroup,
                        runId,
                        runCreationTime,
                        successTime,
                        RunState.SUCCEEDED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotCancelFailedTaskIfNoRetryIsAvailable() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = new TaskConfig(randomTaskGroup(), 1);
        TaskId taskId = taskDao.submitTask(taskConfig);
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
                taskConfig.taskGroup,
                submissionTime,
                failureTime,
                TaskState.FAILED,
                newList(new Run(
                        taskId,
                        taskConfig.taskGroup,
                        runId,
                        runCreationTime,
                        failureTime,
                        RunState.FAILED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldCancelFailedTaskIfRetryIsAvailable() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = new TaskConfig(randomTaskGroup(), randomInt(2, 5));
        TaskId taskId = taskDao.submitTask(taskConfig);
        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        RunId runId = taskDao.createNextAvailableRun().get().runId;
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        taskDao.markAsFailed(runId);

        // When
        ZonedDateTime cancellationTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.cancelTask(taskId);

        // Then
        assertThat(wasSuccess, is(true));
        Task expectedTask = new Task(
                taskId,
                taskConfig.taskGroup,
                submissionTime,
                cancellationTime,
                TaskState.CANCELLED,
                newList(new Run(
                        taskId,
                        taskConfig.taskGroup,
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
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        Run run = taskDao.createNextAvailableRun().get();

        // When
        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsSucceeded(run.runId);

        // Then
        assertThat(wasSuccess, is(true));
        Task expectedTask = new Task(
                taskId,
                taskConfig.taskGroup,
                submissionTime,
                successTime,
                TaskState.SUCCEEDED,
                newList(new Run(
                        taskId,
                        taskConfig.taskGroup,
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
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

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
                taskConfig.taskGroup,
                submissionTime,
                successTime,
                TaskState.SUCCEEDED,
                newList(new Run(
                        taskId,
                        taskConfig.taskGroup,
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
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

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
                taskConfig.taskGroup,
                submissionTime,
                failureTime,
                TaskState.FAILED,
                newList(new Run(
                        taskId,
                        taskConfig.taskGroup,
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
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        Run run = taskDao.createNextAvailableRun().get();

        // When
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsFailed(run.runId);

        // Then
        assertThat(wasSuccess, is(true));
        Task expectedTask = new Task(
                taskId,
                taskConfig.taskGroup,
                submissionTime,
                failureTime,
                TaskState.FAILED,
                newList(new Run(
                        taskId,
                        taskConfig.taskGroup,
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
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

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
                taskConfig.taskGroup,
                submissionTime,
                failureTime,
                TaskState.FAILED,
                newList(new Run(
                        taskId,
                        taskConfig.taskGroup,
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
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

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
                taskConfig.taskGroup,
                submissionTime,
                successTime,
                TaskState.SUCCEEDED,
                newList(new Run(
                        taskId,
                        taskConfig.taskGroup,
                        run.runId,
                        runCreationTime,
                        successTime,
                        RunState.SUCCEEDED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    /* ============= */
    /* --- retry --- */
    /* ============= */

    @Test
    public void shouldRetryFailedTaskIfRunAttemptsAreAvailable() {
        int attemptCount = randomInt(2, 5);

        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = new TaskConfig(randomTaskGroup(), attemptCount);
        TaskId taskId = taskDao.submitTask(taskConfig);
        ZonedDateTime runCreationTime = clock.increaseBy(randomMillis());
        Run run = taskDao.createNextAvailableRun().get();
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        taskDao.markAsFailed(run.runId);

        List<Run> allRuns = newList(new Run(
                taskId,
                taskConfig.taskGroup,
                run.runId,
                runCreationTime,
                failureTime,
                RunState.FAILED
        ));

        // When & Then
        ZonedDateTime lastFailureTime = null;
        for (int attemptNo = 2; attemptNo <= attemptCount; attemptNo++) {

            ZonedDateTime retryTime = clock.increaseBy(randomMillis());
            Optional<Run> lastRun = taskDao.createNextAvailableRun();
            assertThat(lastRun, isPresent());
            RunId lastRunId = lastRun.get().runId;
            Run expectedRun = new Run(
                    taskId,
                    taskConfig.taskGroup,
                    lastRunId,
                    retryTime,
                    retryTime,
                    RunState.CREATED
            );
            assertThat(lastRun, isPresentAndEqualTo(expectedRun));

            List<Run> runsSoFar = newList(allRuns);
            runsSoFar.add(expectedRun);
            Task expectedTask = new Task(
                    taskId,
                    taskConfig.taskGroup,
                    submissionTime,
                    retryTime,
                    TaskState.RUNNING,
                    runsSoFar
            );
            assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));

            lastFailureTime = clock.increaseBy(randomMillis());
            boolean wasSuccess = taskDao.markAsFailed(lastRunId);
            assertThat(wasSuccess, is(true));

            allRuns.add(new Run(
                    taskId,
                    taskConfig.taskGroup,
                    lastRunId,
                    retryTime,
                    lastFailureTime,
                    RunState.FAILED
            ));
            expectedTask = new Task(
                    taskId,
                    taskConfig.taskGroup,
                    submissionTime,
                    lastFailureTime,
                    TaskState.FAILED,
                    allRuns
            );
            assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
        }

        ZonedDateTime failedRetryTime = clock.increaseBy(randomMillis());
        Optional<Run> noRunAnymore = taskDao.createNextAvailableRun();
        assertThat(noRunAnymore, isNotPresent());

        Task expectedTask = new Task(
                taskId,
                taskConfig.taskGroup,
                submissionTime,
                lastFailureTime,
                TaskState.FAILED,
                allRuns
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotRetryOnceTaskBecomesSuccessful() {
        int attemptCount = randomInt(3, 5);

        TaskConfig taskConfig = new TaskConfig(randomTaskGroup(), attemptCount);
        TaskId taskId = taskDao.submitTask(taskConfig);
        Run run1 = taskDao.createNextAvailableRun().get();
        taskDao.markAsFailed(run1.runId);
        Run run2 = taskDao.createNextAvailableRun().get();
        taskDao.markAsSucceeded(run2.runId);

        // When
        Optional<Run> noRunAvailable = taskDao.createNextAvailableRun();

        // Then
        assertThat(noRunAvailable, isNotPresent());
    }


    // todo: verify you can not change state of previous runs

    private TaskConfig commonTaskConfig() {
        return new TaskConfig(randomTaskGroup(), randomInt(1, 5));
    }
}