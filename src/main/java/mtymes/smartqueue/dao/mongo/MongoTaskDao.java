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

    private static final String TASK_ID = "_id";
    private static final String TASK_GROUP = "taskGroup";
    private static final String CREATED_AT_TIME = "createdAt";
    private static final String UPDATED_AT_TIME = "updatedAt";
    protected static final String STATE = "state";

    private static final String IS_AVAILABLE_FOR_EXECUTION = "isAvailable";
    private static final String RUN_ATTEMPTS_LEFT = "attemptsLeft";

    private static final String RUNS = "runs";
    private static final String RUN_ID = "runId";

    private final MongoCollection<Document> tasks;

    private final Clock clock;

    public MongoTaskDao(MongoCollection<Document> tasks, Clock clock) {
        this.tasks = tasks;
        this.clock = clock;
    }

    @Override
    public Optional<Task> loadTask(TaskId taskId) {
        return one(tasks.find(doc(TASK_ID, taskId))).map(this::toTask);
    }

    @Override
    public TaskId submitTask(TaskConfig taskConfig) {
        TaskId taskId = TaskId.taskId(randomUUID());

        ZonedDateTime now = clock.now();
        tasks.insertOne(docBuilder()
                .put(TASK_ID, taskId)
                .put(TASK_GROUP, taskConfig.taskGroup)
                .put(CREATED_AT_TIME, now)
                .put(UPDATED_AT_TIME, now)
                .put(STATE, TaskState.SUBMITTED)
                .put(IS_AVAILABLE_FOR_EXECUTION, true)
                .put(RUN_ATTEMPTS_LEFT, taskConfig.attemptCount)
                .build());

        return taskId;
    }

    @Override
    public boolean cancelTask(TaskId taskId) {
        ZonedDateTime now = clock.now();

        long modifiedCount = tasks.updateOne(
                docBuilder()
                        .put(TASK_ID, taskId)
                        .put(IS_AVAILABLE_FOR_EXECUTION, true)
                        .put(RUN_ATTEMPTS_LEFT, doc("$gt", 0))
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

    // todo: fetch based on taskGroup as well
    @Override
    public Optional<Run> createNextAvailableRun() {
        ZonedDateTime now = clock.now();

        RunId runId = RunId.runId(randomUUID());
        Document document = tasks.findOneAndUpdate(
                // todo: add index for this
                docBuilder()
                        .put(IS_AVAILABLE_FOR_EXECUTION, true)
                        .put(RUN_ATTEMPTS_LEFT, doc("$gt", 0))
                        .build(),
                docBuilder()
                        .put("$addToSet", doc(RUNS, docBuilder()
                                .put(RUN_ID, runId)
                                .put(CREATED_AT_TIME, now)
                                .put(UPDATED_AT_TIME, now)
                                .put(STATE, RunState.CREATED)
                                .build()))
                        .put("$inc", doc(RUN_ATTEMPTS_LEFT, -1))
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
            TaskId taskId = dbTask.getTaskId(TASK_ID);
            TaskGroup taskGroup = dbTask.getTaskGroup(TASK_GROUP);
            DocWrapper dbRun = dbTask.getList(RUNS).lastDoc();
            return toRun(
                    taskId,
                    taskGroup,
                    dbRun
            );
        });
    }

    @Override
    public boolean markAsSucceeded(RunId runId) {
        ZonedDateTime now = clock.now();

        long modifiedCount = tasks.updateOne(
                docBuilder()
                        .put(STATE, TaskState.RUNNING)
                        .put(RUNS, doc("$elemMatch", docBuilder()
                                .put(RUN_ID, runId)
                                .put(STATE, RunState.CREATED)
                                .build()))
                        .build(),
                docBuilder()
                        .put("$set", docBuilder()
                                .put(STATE, TaskState.SUCCEEDED)
                                .put(UPDATED_AT_TIME, now)
                                .put(RUNS + ".$." + STATE, RunState.SUCCEEDED)
                                .put(RUNS + ".$." + UPDATED_AT_TIME, now)
                                .build())
                        .build()
        ).getModifiedCount();

        return modifiedCount == 1;
    }

    @Override
    public boolean markAsFailed(RunId runId) {
        ZonedDateTime now = clock.now();

        long modifiedCount = tasks.updateOne(
                docBuilder()
                        .put(STATE, TaskState.RUNNING)
                        .put(RUNS, doc("$elemMatch", docBuilder()
                                .put(RUN_ID, runId)
                                .put(STATE, RunState.CREATED)
                                .build()))
                        .build(),
                docBuilder()
                        .put("$set", docBuilder()
                                .put(STATE, TaskState.FAILED)
                                .put(UPDATED_AT_TIME, now)
                                .put(RUNS + ".$." + STATE, RunState.FAILED)
                                .put(RUNS + ".$." + UPDATED_AT_TIME, now)
                                .put(IS_AVAILABLE_FOR_EXECUTION, true)
                                .build())
                        .build()
        ).getModifiedCount();

        return modifiedCount == 1;
    }

    private Task toTask(Document doc) {
        DocWrapper dbTask = wrap(doc);

        TaskId taskId = dbTask.getTaskId(TASK_ID);
        TaskGroup taskGroup = dbTask.getTaskGroup(TASK_GROUP);

        return new Task(
                taskId,
                taskGroup,
                dbTask.getZonedDateTime(CREATED_AT_TIME),
                dbTask.getZonedDateTime(UPDATED_AT_TIME),
                dbTask.getTaskState(STATE),
                dbTask.getList(RUNS, true).mapDoc(dbRun -> toRun(taskId, taskGroup, dbRun))
        );
    }

    private Run toRun(TaskId taskId, TaskGroup taskGroup, DocWrapper dbRun) {
        return new Run(
                taskId,
                taskGroup,
                dbRun.getRunId(RUN_ID),
                dbRun.getZonedDateTime(CREATED_AT_TIME),
                dbRun.getZonedDateTime(UPDATED_AT_TIME),
                dbRun.getRunState(STATE)
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
