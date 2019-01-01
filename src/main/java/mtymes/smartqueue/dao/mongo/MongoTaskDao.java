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
    protected static final String STATE = "state";

    private static final String IS_AVAILABLE_FOR_EXECUTION = "isAvailable";
    private static final String EXECUTION_ATTEMPTS_LEFT = "attemptsLeft";

    private static final String EXECUTIONS = "executions";
    private static final String EXECUTION_ID = "executionId";

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

    @Override
    public boolean cancelTask(TaskId taskId) {
        ZonedDateTime now = clock.now();

        long modifiedCount = tasks.updateOne(
                docBuilder()
                        .put(_ID, taskId)
                        .put(IS_AVAILABLE_FOR_EXECUTION, true)
                        .put(EXECUTION_ATTEMPTS_LEFT, doc("$gt", 0))
                        .build(),
                docBuilder()
                        .put("$set", docBuilder()
                                .put(STATE, TaskState.CANCELLED)
                                .put(IS_AVAILABLE_FOR_EXECUTION, false)
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
                // todo: add index for this
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
                        // todo: store lastExecutionId
                        .put("$set", docBuilder()
                                .put(IS_AVAILABLE_FOR_EXECUTION, false)
                                .put(STATE, TaskState.RUNNING)
                                .put(UPDATED_AT_TIME, now)
                                .build())
                        .build(),
                // todo: test this
                sortBy(doc(UPDATED_AT_TIME, 1))
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
                        .put(STATE, TaskState.RUNNING)
                        // todo: executionId must match lastExecutionId
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
                        .put(STATE, TaskState.RUNNING)
                        // todo: executionId must match lastExecutionId
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
                dbTask.getList(EXECUTIONS, true).mapDoc(dbExecution -> toExecution(taskId, dbExecution))
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
