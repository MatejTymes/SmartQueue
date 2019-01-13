package mtymes.common.mongo;

import javafixes.object.DataObject;
import mtymes.common.time.DateUtil;
import mtymes.smartqueue.domain.ExecutionId;
import mtymes.smartqueue.domain.ExecutionState;
import mtymes.smartqueue.domain.TaskId;
import mtymes.smartqueue.domain.TaskState;
import org.bson.Document;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static mtymes.common.time.DateUtil.toZonedDateTime;
import static mtymes.smartqueue.domain.ExecutionId.executionId;
import static mtymes.smartqueue.domain.TaskId.taskId;

// todo: mtymes - move outside of common package
// todo: mtymes - test this
public class DocWrapper extends DataObject {

    private final Document doc;

    public DocWrapper(Document doc) {
        this.doc = doc;
    }

    public static DocWrapper wrap(Document doc) {
        return new DocWrapper(doc);
    }

    public Boolean getBoolean(String fieldName) {
        return getField(fieldName);
    }

    public Integer getInteger(String fieldName) {
        return getField(fieldName);
    }

    public String getString(String fieldName) {
        return getField(fieldName);
    }

    public Optional<String> getOptionalString(String fieldName) {
        return getOptionalField(fieldName);
    }

    public ZonedDateTime getZonedDateTime(String fieldName) {
        Date value = getField(fieldName);
        return toZonedDateTime(value, DateUtil.UTC_ZONE_ID);
    }

    public DocListWrapper getList(String fieldName) {
        List value = getField(fieldName);
        return DocListWrapper.wrap(value);
    }

    public DocListWrapper getOptionalList(String fieldName) {
        Optional<List> value = getOptionalField(fieldName);
        return DocListWrapper.wrap(
                value.orElseGet(Collections::emptyList)
        );
    }

    public TaskId getTaskId(String fieldName) {
        return taskId(getString(fieldName));
    }

    public ExecutionId getExecutionId(String fieldName) {
        return executionId(getString(fieldName));
    }

    public Optional<ExecutionId> getOptionalExecutionId(String fieldName) {
        return getOptionalString(fieldName)
                .map(ExecutionId::executionId);
    }

    public TaskState getTaskState(String fieldName) {
        return TaskState.valueOf(getString(fieldName));
    }

    public ExecutionState getExecutionState(String fieldName) {
        return ExecutionState.valueOf(getString(fieldName));
    }

    private <T> T getField(String fieldName) {
        T value = (T) doc.get(fieldName);
        if (value == null) {
            throw new NullPointerException(format("%s can't be null", fieldName));
        }
        return value;
    }

    private <T> Optional<T> getOptionalField(String fieldName) {
        T value = (T) doc.get(fieldName);
        return Optional.ofNullable(value);
    }
}
