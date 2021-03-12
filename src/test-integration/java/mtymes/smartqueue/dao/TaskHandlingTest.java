package mtymes.smartqueue.dao;

import mtymes.smartqueue.domain.*;
import mtymes.smartqueue.taskHandler.TaskHandler;

import java.time.Duration;
import java.util.Optional;

public abstract class TaskHandlingTest implements TaskHandler {

    protected abstract TaskHandler taskHandler();

    @Override
    public TaskId submitTask() {
        return taskHandler().submitTask();
    }

    @Override
    public TaskId submitTask(TaskConfig taskConfig) {
        return taskHandler().submitTask(taskConfig);
    }

    @Override
    public boolean doesTaskExist(TaskId taskId) {
        return taskHandler().doesTaskExist(taskId);
    }

    @Override
    public boolean cancelTask(TaskId taskId) {
        return taskHandler().cancelTask(taskId);
    }

    @Override
    public boolean cancelTask(TaskId taskId, ExecutionId executionId) {
        return taskHandler().cancelTask(taskId, executionId);
    }

    @Override
    public Optional<Execution> createNextExecution() {
        return taskHandler().createNextExecution();
    }

    @Override
    public boolean markAsSucceeded(ExecutionId executionId) {
        return taskHandler().markAsSucceeded(executionId);
    }

    @Override
    public boolean markAsFailed(ExecutionId executionId) {
        return taskHandler().markAsFailed(executionId);
    }

    @Override
    public TaskState loadTaskState(TaskId taskId) {
        return taskHandler().loadTaskState(taskId);
    }

    @Override
    public ExecutionState loadExecutionState(ExecutionId executionId) {
        return taskHandler().loadExecutionState(executionId);
    }

    @Override
    public ExecutionId loadLastExecutionId(TaskId taskId) {
        return taskHandler().loadLastExecutionId(taskId);
    }

    @Override
    public boolean setTtl(TaskId taskId, Duration ttl) {
        return taskHandler().setTtl(taskId, ttl);
    }

    @Override
    public void waitFor(Duration duration) {
        taskHandler().waitFor(duration);
    }
}
