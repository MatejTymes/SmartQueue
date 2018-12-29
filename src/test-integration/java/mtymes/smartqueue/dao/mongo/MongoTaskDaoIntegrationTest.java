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
                submissionTime,
                submissionTime,
                TaskState.SUBMITTED,
                emptyList()
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    /* ========================== */
    /* --- creating execution --- */
    /* ========================== */

    @Test
    public void shouldNotCreateExecutionIfNoTaskExists() {
        // When
        Optional<Execution> execution = taskDao.createNextExecution();

        // Then
        assertThat(execution, isNotPresent());
    }

    @Test
    public void shouldNotCreateExecutionIfTaskIsCancelled() {
        TaskId taskId = taskDao.submitTask(commonTaskConfig());
        taskDao.cancelTask(taskId);

        // When
        Optional<Execution> execution = taskDao.createNextExecution();

        // Then
        assertThat(execution, isNotPresent());
    }

    @Test
    public void shouldCreateExecutionForSubmittedTask() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

        // When
        ZonedDateTime executionCreationTime = clock.increaseBy(randomMillis());
        Optional<Execution> execution = taskDao.createNextExecution();

        // Then
        assertThat(execution, isPresent());
        Execution expectedExecution = new Execution(
                taskId,
                execution.get().executionId,
                executionCreationTime,
                executionCreationTime,
                ExecutionState.CREATED
        );
        assertThat(execution, isPresentAndEqualTo(expectedExecution));

        Task expectedTask = new Task(
                taskId,
                submissionTime,
                executionCreationTime,
                TaskState.RUNNING,
                newList(expectedExecution)
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
        ZonedDateTime executionCreationTime = clock.increaseBy(randomMillis());
        ExecutionId executionId = taskDao.createNextExecution().get().executionId;
        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        taskDao.markAsSucceeded(executionId);

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
                newList(new Execution(
                        taskId,
                        executionId,
                        executionCreationTime,
                        successTime,
                        ExecutionState.SUCCEEDED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotCancelFailedTaskIfNoRetryIsAvailable() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = new TaskConfig(1);
        TaskId taskId = taskDao.submitTask(taskConfig);
        ZonedDateTime executionCreationTime = clock.increaseBy(randomMillis());
        ExecutionId executionId = taskDao.createNextExecution().get().executionId;
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        taskDao.markAsFailed(executionId);

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
                newList(new Execution(
                        taskId,
                        executionId,
                        executionCreationTime,
                        failureTime,
                        ExecutionState.FAILED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldCancelFailedTaskIfRetryIsAvailable() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = new TaskConfig(randomInt(2, 5));
        TaskId taskId = taskDao.submitTask(taskConfig);
        ZonedDateTime executionCreationTime = clock.increaseBy(randomMillis());
        ExecutionId executionId = taskDao.createNextExecution().get().executionId;
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        taskDao.markAsFailed(executionId);

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
                newList(new Execution(
                        taskId,
                        executionId,
                        executionCreationTime,
                        failureTime,
                        ExecutionState.FAILED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    /* =============== */
    /* --- success --- */
    /* =============== */

    @Test
    public void shouldMarkExecutionAsSucceeded() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

        ZonedDateTime executionCreationTime = clock.increaseBy(randomMillis());
        Execution execution = taskDao.createNextExecution().get();

        // When
        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsSucceeded(execution.executionId);

        // Then
        assertThat(wasSuccess, is(true));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                successTime,
                TaskState.SUCCEEDED,
                newList(new Execution(
                        taskId,
                        execution.executionId,
                        executionCreationTime,
                        successTime,
                        ExecutionState.SUCCEEDED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotMarkExecutionAsSucceededTwice() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

        ZonedDateTime executionCreationTime = clock.increaseBy(randomMillis());
        Execution execution = taskDao.createNextExecution().get();

        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        taskDao.markAsSucceeded(execution.executionId);

        // When
        ZonedDateTime secondSuccessTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsSucceeded(execution.executionId);

        // Then
        assertThat(wasSuccess, is(false));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                successTime,
                TaskState.SUCCEEDED,
                newList(new Execution(
                        taskId,
                        execution.executionId,
                        executionCreationTime,
                        successTime,
                        ExecutionState.SUCCEEDED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotSucceededFailedExecution() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

        ZonedDateTime executionCreationTime = clock.increaseBy(randomMillis());
        Execution execution = taskDao.createNextExecution().get();

        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        taskDao.markAsFailed(execution.executionId);

        // When
        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsSucceeded(execution.executionId);

        // Then
        assertThat(wasSuccess, is(false));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                failureTime,
                TaskState.FAILED,
                newList(new Execution(
                        taskId,
                        execution.executionId,
                        executionCreationTime,
                        failureTime,
                        ExecutionState.FAILED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    /* =============== */
    /* --- failure --- */
    /* =============== */

    @Test
    public void shouldMarkExecutionAsFailed() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

        ZonedDateTime executionCreationTime = clock.increaseBy(randomMillis());
        Execution execution = taskDao.createNextExecution().get();

        // When
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsFailed(execution.executionId);

        // Then
        assertThat(wasSuccess, is(true));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                failureTime,
                TaskState.FAILED,
                newList(new Execution(
                        taskId,
                        execution.executionId,
                        executionCreationTime,
                        failureTime,
                        ExecutionState.FAILED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotMarkExecutionAsFailedTwice() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

        ZonedDateTime executionCreationTime = clock.increaseBy(randomMillis());
        Execution execution = taskDao.createNextExecution().get();

        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        taskDao.markAsFailed(execution.executionId);

        // When
        ZonedDateTime secondFailureTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsFailed(execution.executionId);

        // Then
        assertThat(wasSuccess, is(false));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                failureTime,
                TaskState.FAILED,
                newList(new Execution(
                        taskId,
                        execution.executionId,
                        executionCreationTime,
                        failureTime,
                        ExecutionState.FAILED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotFailSucceededExecution() {
        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = commonTaskConfig();
        TaskId taskId = taskDao.submitTask(taskConfig);

        ZonedDateTime executionCreationTime = clock.increaseBy(randomMillis());
        Execution execution = taskDao.createNextExecution().get();

        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        taskDao.markAsSucceeded(execution.executionId);

        // When
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        boolean wasSuccess = taskDao.markAsFailed(execution.executionId);

        // Then
        assertThat(wasSuccess, is(false));
        Task expectedTask = new Task(
                taskId,
                submissionTime,
                successTime,
                TaskState.SUCCEEDED,
                newList(new Execution(
                        taskId,
                        execution.executionId,
                        executionCreationTime,
                        successTime,
                        ExecutionState.SUCCEEDED
                ))
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    /* ============= */
    /* --- retry --- */
    /* ============= */

    @Test
    public void shouldRetryFailedTaskIfExecutionAttemptsAreAvailable() {
        int attemptCount = randomInt(2, 5);

        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskConfig taskConfig = new TaskConfig(attemptCount);
        TaskId taskId = taskDao.submitTask(taskConfig);
        ZonedDateTime executionCreationTime = clock.increaseBy(randomMillis());
        Execution execution = taskDao.createNextExecution().get();
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        taskDao.markAsFailed(execution.executionId);

        List<Execution> allExecutions = newList(new Execution(
                taskId,
                execution.executionId,
                executionCreationTime,
                failureTime,
                ExecutionState.FAILED
        ));

        // When & Then
        ZonedDateTime lastFailureTime = null;
        for (int attemptNo = 2; attemptNo <= attemptCount; attemptNo++) {

            ZonedDateTime retryTime = clock.increaseBy(randomMillis());
            Optional<Execution> lastExecution = taskDao.createNextExecution();
            assertThat(lastExecution, isPresent());
            ExecutionId lastExecutionId = lastExecution.get().executionId;
            Execution expectedExecution = new Execution(
                    taskId,
                    lastExecutionId,
                    retryTime,
                    retryTime,
                    ExecutionState.CREATED
            );
            assertThat(lastExecution, isPresentAndEqualTo(expectedExecution));

            List<Execution> executionsSoFar = newList(allExecutions);
            executionsSoFar.add(expectedExecution);
            Task expectedTask = new Task(
                    taskId,
                    submissionTime,
                    retryTime,
                    TaskState.RUNNING,
                    executionsSoFar
            );
            assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));

            lastFailureTime = clock.increaseBy(randomMillis());
            boolean wasSuccess = taskDao.markAsFailed(lastExecutionId);
            assertThat(wasSuccess, is(true));

            allExecutions.add(new Execution(
                    taskId,
                    lastExecutionId,
                    retryTime,
                    lastFailureTime,
                    ExecutionState.FAILED
            ));
            expectedTask = new Task(
                    taskId,
                    submissionTime,
                    lastFailureTime,
                    TaskState.FAILED,
                    allExecutions
            );
            assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
        }

        ZonedDateTime failedRetryTime = clock.increaseBy(randomMillis());
        Optional<Execution> noExecutionAnymore = taskDao.createNextExecution();
        assertThat(noExecutionAnymore, isNotPresent());

        Task expectedTask = new Task(
                taskId,
                submissionTime,
                lastFailureTime,
                TaskState.FAILED,
                allExecutions
        );
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTask));
    }

    @Test
    public void shouldNotRetryOnceTaskBecomesSuccessful() {
        int attemptCount = randomInt(3, 5);

        TaskConfig taskConfig = new TaskConfig(attemptCount);
        TaskId taskId = taskDao.submitTask(taskConfig);
        Execution execution1 = taskDao.createNextExecution().get();
        taskDao.markAsFailed(execution1.executionId);
        Execution execution2 = taskDao.createNextExecution().get();
        taskDao.markAsSucceeded(execution2.executionId);

        // When
        Optional<Execution> noExecutionAvailable = taskDao.createNextExecution();

        // Then
        assertThat(noExecutionAvailable, isNotPresent());
    }


    // todo: verify you can not change state of previous executions

    private TaskConfig commonTaskConfig() {
        return new TaskConfig(randomInt(1, 5));
    }
}