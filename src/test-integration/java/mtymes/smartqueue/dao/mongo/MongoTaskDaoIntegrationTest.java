package mtymes.smartqueue.dao.mongo;

import com.mongodb.client.MongoDatabase;
import mtymes.smartqueue.dao.BaseTaskTest;
import mtymes.smartqueue.domain.*;
import mtymes.test.db.EmbeddedDB;
import mtymes.test.db.MongoManager;
import mtymes.test.time.FixedClock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.emptyList;
import static javafixes.common.CollectionUtil.newList;
import static mtymes.common.time.DateUtil.UTC_ZONE_ID;
import static mtymes.smartqueue.dao.mongo.MongoCollections.bodiesCollection;
import static mtymes.smartqueue.dao.mongo.MongoCollections.tasksCollection;
import static mtymes.smartqueue.domain.TaskConfigBuilder.taskConfigBuilder;
import static mtymes.test.OptionalMatcher.isPresentAndEqualTo;
import static mtymes.test.Random.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

//todo: maybe start recording ttl for testing purposes
public class MongoTaskDaoIntegrationTest extends BaseTaskTest {

    private static EmbeddedDB db;
    private static FixedClock clock = new FixedClock();

    private static MongoTaskDao taskDao;

    private Map<TaskId, Task> expectedTasks = newHashMap();
    private Map<TaskId, TaskBody> expectedTaskBodies = newHashMap();
    private Map<ExecutionId, TaskId> executionIdToTaskIdMap = newHashMap();

    @BeforeClass
    public static void initDB() {
        db = MongoManager.getEmbeddedDB();
        MongoDatabase database = db.getDatabase();
        taskDao = new MongoTaskDao(
                tasksCollection(database),
                bodiesCollection(database),
                clock
        );
    }

    @Before
    public void setUp() {
        db.removeAllData();

        expectedTasks.clear();
        expectedTaskBodies.clear();
        executionIdToTaskIdMap.clear();

        clock.setNow(ZonedDateTime.now(UTC_ZONE_ID));
    }

    @AfterClass
    public static void releaseDB() {
        MongoManager.release(db);
    }


    @Override
    protected TaskId submitTask() {
        return submitTask(
                taskConfigBuilder()
                        .attemptCount(randomInt(1, 5))
                        .build()
        );
    }

    @Override
    protected TaskId submitTask(TaskConfig taskConfig) {
        TaskBody taskBody = randomTaskBody();

        ZonedDateTime submissionTime = clock.increaseBy(randomMillis());
        TaskId taskId = taskDao.submitTask(taskConfig, taskBody);

        expectedTasks.put(taskId, new Task(
                taskId,
                submissionTime,
                submissionTime,
                TaskState.SUBMITTED,
                Optional.empty(),
                emptyList()
        ));
        expectedTaskBodies.put(taskId, taskBody);

        verifyEverythingIsAsExpected(taskId);

        return taskId;
    }

    @Override
    protected boolean doesTaskExist(TaskId taskId) {
        return taskDao.loadTask(taskId).isPresent();
    }

    @Override
    protected boolean cancelTask(TaskId taskId) {
        return cancelTask(taskId, Optional.empty());
    }

    @Override
    protected boolean cancelTask(TaskId taskId, ExecutionId executionId) {
        return cancelTask(taskId, Optional.of(executionId));
    }

    private boolean cancelTask(TaskId taskId, Optional<ExecutionId> executionId) {
        ZonedDateTime cancellationTime = clock.increaseBy(randomMillis());
        boolean wasApplied = taskDao.cancelTask(taskId, executionId);

        if (wasApplied) {
            Task taskInPreviousState = expectedTasks.get(taskId);
            expectedTasks.put(taskId, new Task(
                    taskId,
                    taskInPreviousState.submittedAt,
                    cancellationTime,
                    TaskState.CANCELLED,
                    taskInPreviousState.lastExecutionId,
                    taskInPreviousState.executions
            ));
        }

        verifyEverythingIsAsExpected(taskId);

        return wasApplied;
    }

    @Override
    protected Optional<Execution> createNextExecution() {
        ZonedDateTime executionCreationTime = clock.increaseBy(randomMillis());
        Optional<Execution> execution = taskDao.createNextExecution();

        if (execution.isPresent()) {
            Execution createdExecution = execution.get();
            TaskId taskId = createdExecution.taskId;
            executionIdToTaskIdMap.put(createdExecution.executionId, taskId);

            Execution expectedExecution = new Execution(
                    taskId,
                    createdExecution.executionId,
                    executionCreationTime,
                    executionCreationTime,
                    ExecutionState.CREATED
            );
            assertThat(createdExecution, is(expectedExecution));

            Task taskInPreviousState = expectedTasks.get(taskId);
            List<Execution> newExecutions = newList(taskInPreviousState.executions);
            newExecutions.add(createdExecution);
            expectedTasks.put(taskId, new Task(
                    taskId,
                    taskInPreviousState.submittedAt,
                    executionCreationTime,
                    TaskState.RUNNING,
                    Optional.of(createdExecution.executionId),
                    newExecutions
            ));

            verifyEverythingIsAsExpected(taskId);
        }

        return execution;
    }

    @Override
    protected boolean markAsSucceeded(ExecutionId executionId) {
        ZonedDateTime successTime = clock.increaseBy(randomMillis());
        boolean wasApplied = taskDao.markAsSucceeded(executionId);

        if (wasApplied) {
            TaskId taskId = executionIdToTaskIdMap.get(executionId);
            Task taskInPreviousState = expectedTasks.get(taskId);

            assertThat(taskInPreviousState.lastExecutionId, isPresentAndEqualTo(executionId));

            List<Execution> executionsInPreviousState = taskInPreviousState.executions;
            Execution executionInPreviousState = executionsInPreviousState.get(executionsInPreviousState.size() - 1);
            Execution expectedExecution = new Execution(
                    taskId,
                    executionId,
                    executionInPreviousState.createdAt,
                    successTime,
                    ExecutionState.SUCCEEDED
            );
            List<Execution> expectedExecutions = newList(executionsInPreviousState);
            expectedExecutions.set(expectedExecutions.size() - 1, expectedExecution);
            expectedTasks.put(taskId, new Task(
                    taskId,
                    taskInPreviousState.submittedAt,
                    successTime,
                    TaskState.SUCCEEDED,
                    taskInPreviousState.lastExecutionId,
                    expectedExecutions
            ));
        }

        verifyEverythingIsAsExpected(executionIdToTaskIdMap.get(executionId));

        return wasApplied;
    }

    @Override
    protected boolean markAsFailed(ExecutionId executionId) {
        ZonedDateTime failureTime = clock.increaseBy(randomMillis());
        boolean wasApplied = taskDao.markAsFailed(executionId);

        if (wasApplied) {
            TaskId taskId = executionIdToTaskIdMap.get(executionId);
            Task taskInPreviousState = expectedTasks.get(taskId);

            assertThat(taskInPreviousState.lastExecutionId, isPresentAndEqualTo(executionId));

            List<Execution> executionsInPreviousState = taskInPreviousState.executions;
            Execution executionInPreviousState = executionsInPreviousState.get(executionsInPreviousState.size() - 1);
            Execution expectedExecution = new Execution(
                    taskId,
                    executionId,
                    executionInPreviousState.createdAt,
                    failureTime,
                    ExecutionState.FAILED
            );
            List<Execution> expectedExecutions = newList(executionsInPreviousState);
            expectedExecutions.set(expectedExecutions.size() - 1, expectedExecution);
            expectedTasks.put(taskId, new Task(
                    taskId,
                    taskInPreviousState.submittedAt,
                    failureTime,
                    TaskState.FAILED,
                    taskInPreviousState.lastExecutionId,
                    expectedExecutions
            ));
        }

        verifyEverythingIsAsExpected(executionIdToTaskIdMap.get(executionId));

        return wasApplied;
    }

    @Override
    protected TaskState loadTaskState(TaskId taskId) {
        Optional<Task> task = taskDao.loadTask(taskId);
        if (task.isPresent()) {
            return task.get().state;
        }
        throw new AssertionError("Task for TaskId '" + taskId + "' not found");
    }

    @Override
    protected ExecutionState loadExecutionState(ExecutionId executionId) {
        TaskId taskId = executionIdToTaskIdMap.get(executionId);
        if (taskId != null) {
            Optional<Task> task = taskDao.loadTask(taskId);
            if (task.isPresent()) {
                for (Execution execution : task.get().executions) {
                    if (executionId.equals(execution.executionId)) {
                        return execution.state;
                    }
                }
            }
        }
        throw new AssertionError("Execution for ExecutionId '" + executionId + "' not found");
    }

    @Override
    protected ExecutionId loadLastExecutionId(TaskId taskId) {
        Optional<Task> task = taskDao.loadTask(taskId);
        if (task.isPresent()) {
            Optional<ExecutionId> lastExecutionId = task.get().lastExecutionId;
            if (lastExecutionId.isPresent()) {
                return lastExecutionId.get();
            }
            throw new AssertionError("Task with TaskId '" + taskId + "' has no Execution");
        }
        throw new AssertionError("Task for TaskId '" + taskId + "' not found");
    }

    @Override
    protected boolean setTtl(TaskId taskId, Duration ttl) {
        return taskDao.setTTL(taskId, ttl);
    }

    @Override
    protected void waitFor(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void verifyEverythingIsAsExpected(TaskId taskId) {
        // todo: maybe verify all tasks
        assertThat(taskDao.loadTask(taskId), isPresentAndEqualTo(expectedTasks.get(taskId)));
        assertThat(taskDao.loadTaskBody(taskId), isPresentAndEqualTo(expectedTaskBodies.get(taskId)));
    }
}
