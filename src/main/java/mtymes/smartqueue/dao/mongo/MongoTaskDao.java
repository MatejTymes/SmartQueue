package mtymes.smartqueue.dao.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import mtymes.common.mongo.DocWrapper;
import mtymes.common.time.Clock;
import mtymes.smartqueue.dao.TaskDao;
import mtymes.smartqueue.domain.*;
import org.bson.Document;

import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.Optional;

import static com.mongodb.client.model.ReturnDocument.AFTER;
import static java.util.UUID.randomUUID;
import static mtymes.common.mongo.DocBuilder.doc;
import static mtymes.common.mongo.DocBuilder.docBuilder;
import static mtymes.common.mongo.DocWrapper.wrap;

public class MongoTaskDao implements TaskDao {

    private static final String _ID = "_id";
    private static final String CREATED_AT_TIME = "createdAt";
    private static final String UPDATED_AT_TIME = "updatedAt";
    private static final String STATE = "state";

    static final String IS_AVAILABLE_FOR_EXECUTION = "isAvailable";
    static final String AVAILABLE_SINCE = "availableSince";
    static final String EXECUTION_ATTEMPTS_LEFT = "attemptsLeft";

    private static final String EXECUTIONS = "executions";
    private static final String EXECUTION_ID = "executionId";
    static final String LAST_EXECUTION_ID = "lastExecutionId";

    private static final String CONTENT = "content";

    private final MongoCollection<Document> tasks;
    private final MongoCollection<Document> bodies;

    private final Clock clock;

    public MongoTaskDao(MongoCollection<Document> tasks, MongoCollection<Document> bodies, Clock clock) {
        this.tasks = tasks;
        this.bodies = bodies;
        this.clock = clock;
    }

    @Override
    public TaskId submitTask(TaskConfig config, TaskBody body) {
        TaskId taskId = TaskId.taskId(randomUUID());

        ZonedDateTime now = clock.now();
        bodies.insertOne(docBuilder()
                .put(_ID, taskId)
                .put(CONTENT, body.content)
                .put(CREATED_AT_TIME, now)
                .build());
        tasks.insertOne(docBuilder()
                .put(_ID, taskId)
                .put(CREATED_AT_TIME, now)
                .put(UPDATED_AT_TIME, now)
                .put(STATE, TaskState.SUBMITTED)
                .put(IS_AVAILABLE_FOR_EXECUTION, true)
                .put(AVAILABLE_SINCE, now)
                .put(EXECUTION_ATTEMPTS_LEFT, config.attemptCount)
                .build());

        return taskId;
    }

    @Override
    public Optional<Task> loadTask(TaskId taskId) {
        return one(tasks.find(doc(_ID, taskId))).map(this::toTask);
    }

    @Override
    public Optional<TaskBody> loadTaskBody(TaskId taskId) {
        return one(bodies.find(doc(_ID, taskId))).map(this::toTaskBody);
    }

    // todo: test the Optional<ExecutionId> lastAssumedExecutionId
    @Override
    public boolean cancelTask(TaskId taskId, Optional<ExecutionId> lastAssumedExecutionId) {
        ZonedDateTime now = clock.now();

        long modifiedCount = tasks.updateOne(
                docBuilder()
                        .put(_ID, taskId)
                        .put(LAST_EXECUTION_ID, lastAssumedExecutionId.orElse(null)) // todo: test this
                        .put(IS_AVAILABLE_FOR_EXECUTION, true)
                        .put(EXECUTION_ATTEMPTS_LEFT, doc("$gt", 0))
                        .build(),
                docBuilder()
                        .put("$set", docBuilder()
                                .put(STATE, TaskState.CANCELLED)
                                .put(IS_AVAILABLE_FOR_EXECUTION, false)
                                .put(AVAILABLE_SINCE, null)
                                .put(UPDATED_AT_TIME, now)
                                .build())
                        .build()
        ).getModifiedCount();

        return modifiedCount == 1;
    }

    @Override
    public Optional<Execution> createNextExecution() {
        ZonedDateTime now = clock.now();

        ExecutionId executionId = ExecutionId.executionId(randomUUID());
        Document document = tasks.findOneAndUpdate(
                docBuilder()
                        .put(IS_AVAILABLE_FOR_EXECUTION, true)
                        .put(EXECUTION_ATTEMPTS_LEFT, doc("$gt", 0))
                        .build(),
                docBuilder()
                        .put("$addToSet", doc(EXECUTIONS, docBuilder()
                                .put(EXECUTION_ID, executionId)
                                .put(CREATED_AT_TIME, now)
                                .put(UPDATED_AT_TIME, now)
                                .put(STATE, ExecutionState.CREATED)
                                .build()))
                        .put("$inc", doc(EXECUTION_ATTEMPTS_LEFT, -1))
                        .put("$set", docBuilder()
                                .put(IS_AVAILABLE_FOR_EXECUTION, false)
                                .put(AVAILABLE_SINCE, null)
                                .put(STATE, TaskState.RUNNING)
                                .put(LAST_EXECUTION_ID, executionId)
                                .put(UPDATED_AT_TIME, now)
                                .build())
                        .build(),
                // todo: test this
                sortBy(doc(AVAILABLE_SINCE, 1))
        );

        return Optional.ofNullable(document).map(doc -> {
            DocWrapper dbTask = wrap(doc);
            TaskId taskId = dbTask.getTaskId(_ID);

            DocWrapper dbExecution = dbTask.getList(EXECUTIONS).lastDoc();
            return toExecution(
                    taskId,
                    dbExecution
            );
        });
    }

    @Override
    public boolean markAsSucceeded(ExecutionId executionId) {
        ZonedDateTime now = clock.now();

        long modifiedCount = tasks.updateOne(
                docBuilder()
                        .put(LAST_EXECUTION_ID, executionId)
                        .put(STATE, TaskState.RUNNING)
                        .put(EXECUTIONS, doc("$elemMatch", docBuilder()
                                .put(EXECUTION_ID, executionId)
                                .put(STATE, ExecutionState.CREATED)
                                .build()))
                        .build(),
                docBuilder()
                        .put("$set", docBuilder()
                                .put(STATE, TaskState.SUCCEEDED)
                                .put(UPDATED_AT_TIME, now)
                                .put(EXECUTIONS + ".$." + STATE, ExecutionState.SUCCEEDED)
                                .put(EXECUTIONS + ".$." + UPDATED_AT_TIME, now)
                                .build())
                        .build()
        ).getModifiedCount();

        return modifiedCount == 1;
    }

    @Override
    public boolean markAsFailed(ExecutionId executionId) {
        ZonedDateTime now = clock.now();

        long modifiedCount = tasks.updateOne(
                docBuilder()
                        .put(LAST_EXECUTION_ID, executionId)
                        .put(STATE, TaskState.RUNNING)
                        .put(EXECUTIONS, doc("$elemMatch", docBuilder()
                                .put(EXECUTION_ID, executionId)
                                .put(STATE, ExecutionState.CREATED)
                                .build()))
                        .build(),
                docBuilder()
                        .put("$set", docBuilder()
                                .put(STATE, TaskState.FAILED)
                                .put(UPDATED_AT_TIME, now)
                                .put(EXECUTIONS + ".$." + STATE, ExecutionState.FAILED)
                                .put(EXECUTIONS + ".$." + UPDATED_AT_TIME, now)
                                .put(IS_AVAILABLE_FOR_EXECUTION, true)
                                .put(AVAILABLE_SINCE, now)
                                .build())
                        .build()
        ).getModifiedCount();

        return modifiedCount == 1;
    }

    private Task toTask(Document doc) {
        DocWrapper dbTask = wrap(doc);

        TaskId taskId = dbTask.getTaskId(_ID);

        return new Task(
                taskId,
                dbTask.getZonedDateTime(CREATED_AT_TIME),
                dbTask.getZonedDateTime(UPDATED_AT_TIME),
                dbTask.getTaskState(STATE),
                dbTask.getOptionalExecutionId(LAST_EXECUTION_ID),
                dbTask.getOptionalList(EXECUTIONS).mapDoc(dbExecution -> toExecution(taskId, dbExecution))
        );
    }

    private TaskBody toTaskBody(Document doc) {
        DocWrapper dbTaskBody = wrap(doc);

        return new TaskBody(
                dbTaskBody.getString(CONTENT)
        );
    }

    private Execution toExecution(TaskId taskId, DocWrapper dbExecution) {
        return new Execution(
                taskId,
                dbExecution.getExecutionId(EXECUTION_ID),
                dbExecution.getZonedDateTime(CREATED_AT_TIME),
                dbExecution.getZonedDateTime(UPDATED_AT_TIME),
                dbExecution.getExecutionState(STATE)
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
