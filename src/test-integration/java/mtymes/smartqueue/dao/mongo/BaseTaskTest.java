package mtymes.smartqueue.dao.mongo;

import mtymes.smartqueue.domain.*;
import org.junit.Test;

import java.util.Optional;

import static mtymes.smartqueue.domain.TaskConfigBuilder.taskConfigBuilder;
import static mtymes.test.OptionalMatcher.isNotPresent;
import static mtymes.test.OptionalMatcher.isPresent;
import static mtymes.test.Random.randomExecutionId;
import static mtymes.test.Random.randomInt;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public abstract class BaseTaskTest {

    protected abstract TaskId submitTask();

    protected abstract TaskId submitTask(TaskConfig taskConfig);

    protected abstract boolean cancelTask(TaskId taskId);

    protected abstract boolean cancelTask(TaskId taskId, ExecutionId executionId);

    protected abstract Optional<Execution> createNextExecution();

    protected abstract boolean markAsSucceeded(ExecutionId executionId);

    protected abstract boolean markAsFailed(ExecutionId executionId);

    protected abstract TaskState loadTaskState(TaskId taskId);

    protected abstract ExecutionState loadExecutionState(ExecutionId executionId);

    protected abstract ExecutionId loadLastExecutionId(TaskId taskId);

    @Test
    public void shouldNotCreateExecutionIfNoTaskExists() {
        // When
        Optional<Execution> execution = createNextExecution();

        // Then
        assertThat(execution, isNotPresent());
    }

    @Test
    public void shouldNotCreateExecutionIfTaskIsCancelled() {
        TaskId taskId = submitTask();
        cancelTask(taskId);

        // When
        Optional<Execution> execution = createNextExecution();

        // Then
        assertThat(execution, isNotPresent());
    }

    @Test
    public void shouldCreateExecutionForSubmittedTask() {
        TaskId taskId = submitTask();

        // When
        Optional<Execution> execution = createNextExecution();

        // Then
        assertThat(execution, isPresent());
        assertThat(execution.get().taskId, is(taskId));
        assertThat(loadTaskState(taskId), is(TaskState.RUNNING));
        assertThat(loadExecutionState(execution.get().executionId), is(ExecutionState.CREATED));
        assertThat(loadLastExecutionId(taskId), is(execution.get().executionId));
    }

    /* ==================== */
    /* --- cancellation --- */
    /* ==================== */

    @Test
    public void shouldCancelTaskWithoutAnyExecution() {
        TaskId taskId = submitTask();

        // When
        boolean wasApplied = cancelTask(taskId);

        // Then
        assertThat(wasApplied, is(true));
        assertThat(loadTaskState(taskId), is(TaskState.CANCELLED));
        assertThat(createNextExecution(), isNotPresent());
    }

    @Test
    public void shouldNotCancelTaskTwice() {
        TaskId taskId = submitTask();
        cancelTask(taskId);

        // When
        boolean wasApplied = cancelTask(taskId);

        // Then
        assertThat(wasApplied, is(false));
        assertThat(loadTaskState(taskId), is(TaskState.CANCELLED));
    }

    @Test
    public void shouldNotCancelTaskWithoutAnyExecutionIfWrongLastExecutionIdIsProvided() {
        TaskId taskId = submitTask();

        // When
        ExecutionId randomExecutionId = randomExecutionId();
        boolean wasApplied = cancelTask(taskId, randomExecutionId);

        // Then
        assertThat(wasApplied, is(false));
        assertThat(loadTaskState(taskId), is(TaskState.SUBMITTED));
    }

    @Test
    public void shouldNotCancelSuccessfulTask() {
        TaskId taskId = submitTask();
        ExecutionId lastExecutionId = createNextExecution().get().executionId;
        markAsSucceeded(lastExecutionId);

        // When
        boolean wasApplied = cancelTask(taskId, lastExecutionId);

        // Then
        assertThat(wasApplied, is(false));
        assertThat(loadTaskState(taskId), is(TaskState.SUCCEEDED));
        assertThat(loadExecutionState(lastExecutionId), is(ExecutionState.SUCCEEDED));
    }

    @Test
    public void shouldNotCancelFailedTaskIfNoRetryIsAvailable() {
        TaskId taskId = submitTask(taskConfigBuilder()
                .attemptCount(1)
                .build());
        ExecutionId lastExecutionId = createNextExecution().get().executionId;
        markAsFailed(lastExecutionId);

        // When
        boolean wasApplied = cancelTask(taskId, lastExecutionId);

        // Then
        assertThat(wasApplied, is(false));
        assertThat(loadTaskState(taskId), is(TaskState.FAILED));
        assertThat(loadExecutionState(lastExecutionId), is(ExecutionState.FAILED));
    }


    @Test
    public void shouldCancelFailedTaskIfRetryIsAvailable() {
        TaskId taskId = submitTask(taskConfigBuilder()
                .attemptCount(randomInt(2, 5))
                .build());
        ExecutionId lastExecutionId = createNextExecution().get().executionId;
        markAsFailed(lastExecutionId);

        // When
        boolean wasApplied = cancelTask(taskId, lastExecutionId);

        // Then
        assertThat(wasApplied, is(true));
        assertThat(loadTaskState(taskId), is(TaskState.CANCELLED));
        assertThat(loadExecutionState(lastExecutionId), is(ExecutionState.FAILED));
        assertThat(createNextExecution(), isNotPresent());
    }

    @Test
    public void shouldNotCancelFailedTaskIfRetryIsAvailableButNoLastExecutionIdIsProvided() {
        TaskId taskId = submitTask(taskConfigBuilder()
                .attemptCount(randomInt(2, 5))
                .build());
        ExecutionId lastExecutionId = createNextExecution().get().executionId;
        markAsFailed(lastExecutionId);

        // When
        boolean wasApplied = cancelTask(taskId); // no last ExecutionId provided

        // Then
        assertThat(wasApplied, is(false));
        assertThat(loadTaskState(taskId), is(TaskState.FAILED));
        assertThat(loadExecutionState(lastExecutionId), is(ExecutionState.FAILED));
    }

    @Test
    public void shouldNotCancelFailedTaskIfRetryIsAvailableButWrongLastExecutionIdIsUsed() {
        TaskId taskId = submitTask(taskConfigBuilder()
                .attemptCount(randomInt(2, 5))
                .build());
        ExecutionId lastExecutionId = createNextExecution().get().executionId;
        markAsFailed(lastExecutionId);

        // When
        ExecutionId randomExecutionId = randomExecutionId();
        boolean wasApplied = cancelTask(taskId, randomExecutionId);

        // Then
        assertThat(wasApplied, is(false));
        assertThat(loadTaskState(taskId), is(TaskState.FAILED));
        assertThat(loadExecutionState(lastExecutionId), is(ExecutionState.FAILED));
    }

    @Test
    public void shouldNotCancelFailedTaskIfRetryIsAvailableButOldLastExecutionIdIsUsed() {
        TaskId taskId = submitTask(taskConfigBuilder()
                .attemptCount(randomInt(3, 5))
                .build());

        ExecutionId firstExecutionId = createNextExecution().get().executionId;
        markAsFailed(firstExecutionId);

        ExecutionId lastExecutionId = createNextExecution().get().executionId;
        markAsFailed(lastExecutionId);

        // When
        ExecutionId wrongLastExecutionId = firstExecutionId;
        boolean wasApplied = cancelTask(taskId, wrongLastExecutionId);

        // Then
        assertThat(wasApplied, is(false));
        assertThat(loadTaskState(taskId), is(TaskState.FAILED));
        assertThat(loadExecutionState(lastExecutionId), is(ExecutionState.FAILED));
    }

    /* =============== */
    /* --- success --- */
    /* =============== */

    @Test
    public void shouldMarkExecutionAsSucceeded() {
        TaskId taskId = submitTask();
        ExecutionId executionId = createNextExecution().get().executionId;

        // When
        boolean wasApplied = markAsSucceeded(executionId);

        // Then
        assertThat(wasApplied, is(true));
        assertThat(loadTaskState(taskId), is(TaskState.SUCCEEDED));
        assertThat(loadExecutionState(executionId), is(ExecutionState.SUCCEEDED));
    }

    @Test
    public void shouldNotMarkExecutionAsSucceededTwice() {
        TaskId taskId = submitTask();
        ExecutionId executionId = createNextExecution().get().executionId;
        markAsSucceeded(executionId);

        // When
        boolean wasApplied = markAsSucceeded(executionId);

        // Then
        assertThat(wasApplied, is(false));
        assertThat(loadTaskState(taskId), is(TaskState.SUCCEEDED));
        assertThat(loadExecutionState(executionId), is(ExecutionState.SUCCEEDED));
    }

    @Test
    public void shouldNotSucceededFailedExecution() {
        TaskId taskId = submitTask();
        ExecutionId executionId = createNextExecution().get().executionId;
        markAsFailed(executionId);

        // When
        boolean wasApplied = markAsSucceeded(executionId);

        // Then
        assertThat(wasApplied, is(false));
        assertThat(loadTaskState(taskId), is(TaskState.FAILED));
        assertThat(loadExecutionState(executionId), is(ExecutionState.FAILED));
    }

    /* =============== */
    /* --- failure --- */
    /* =============== */

    @Test
    public void shouldMarkExecutionAsFailed() {
        TaskId taskId = submitTask();
        ExecutionId executionId = createNextExecution().get().executionId;

        // When
        boolean wasApplied = markAsFailed(executionId);

        // Then
        assertThat(wasApplied, is(true));
        assertThat(loadTaskState(taskId), is(TaskState.FAILED));
        assertThat(loadExecutionState(executionId), is(ExecutionState.FAILED));
    }

    @Test
    public void shouldNotMarkExecutionAsFailedTwice() {
        TaskId taskId = submitTask();
        ExecutionId executionId = createNextExecution().get().executionId;
        markAsFailed(executionId);

        // When
        boolean wasApplied = markAsFailed(executionId);

        // Then
        assertThat(wasApplied, is(false));
        assertThat(loadTaskState(taskId), is(TaskState.FAILED));
        assertThat(loadExecutionState(executionId), is(ExecutionState.FAILED));
    }


    @Test
    public void shouldNotFailSucceededExecution() {
        TaskId taskId = submitTask();
        ExecutionId executionId = createNextExecution().get().executionId;
        markAsSucceeded(executionId);

        // When
        boolean wasApplied = markAsFailed(executionId);

        // Then
        assertThat(wasApplied, is(false));
        assertThat(loadTaskState(taskId), is(TaskState.SUCCEEDED));
        assertThat(loadExecutionState(executionId), is(ExecutionState.SUCCEEDED));
    }

    /* ============= */
    /* --- retry --- */
    /* ============= */

    @Test
    public void shouldRetryFailedTaskIfExecutionAttemptsAreAvailable() {
        int attemptCount = randomInt(2, 5);

        TaskId taskId = submitTask(taskConfigBuilder()
                .attemptCount(attemptCount)
                .build());
        ExecutionId executionId = createNextExecution().get().executionId;
        markAsFailed(executionId);

        // When & Then
        for (int attemptNo = 2; attemptNo <= attemptCount; attemptNo++) {
            Optional<Execution> nextExecution = createNextExecution();

            assertThat(nextExecution, isPresent());
            ExecutionId lastExecutionId = nextExecution.get().executionId;

            assertThat(nextExecution.get().taskId, is(taskId));
            assertThat(loadTaskState(taskId), is(TaskState.RUNNING));
            assertThat(loadExecutionState(lastExecutionId), is(ExecutionState.CREATED));
            assertThat(loadLastExecutionId(taskId), is(nextExecution.get().executionId));

            boolean wasApplied = markAsFailed(lastExecutionId);

            assertThat(wasApplied, is(true));
            assertThat(loadTaskState(taskId), is(TaskState.FAILED));
            assertThat(loadExecutionState(lastExecutionId), is(ExecutionState.FAILED));
        }

        Optional<Execution> noExecutionAnymore = createNextExecution();
        assertThat(noExecutionAnymore, isNotPresent());

        assertThat(loadTaskState(taskId), is(TaskState.FAILED));
    }

    @Test
    public void shouldNotRetryOnceTaskBecomesSuccessful() {
        int attemptCount = randomInt(3, 5);

        TaskId taskId = submitTask(taskConfigBuilder()
                .attemptCount(attemptCount)
                .build());
        ExecutionId executionId1 = createNextExecution().get().executionId;
        markAsFailed(executionId1);
        assertThat(loadLastExecutionId(taskId), is(executionId1));

        ExecutionId executionId2 = createNextExecution().get().executionId;
        markAsSucceeded(executionId2);
        assertThat(loadLastExecutionId(taskId), is(executionId2));

        // When
        Optional<Execution> noExecutionAnymore = createNextExecution();

        // Then
        assertThat(noExecutionAnymore, isNotPresent());
        assertThat(loadTaskState(taskId), is(TaskState.SUCCEEDED));
    }

    // todo: verify you can not change state of previous executions
}
