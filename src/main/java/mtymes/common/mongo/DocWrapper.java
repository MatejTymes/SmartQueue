package mtymes.common.mongo;

import javafixes.object.DataObject;
import mtymes.common.time.DateUtil;
import mtymes.smartqueue.domain.JobId;
import mtymes.smartqueue.domain.JobRequestId;
import mtymes.smartqueue.domain.JobRequestState;
import mtymes.smartqueue.domain.JobState;
import org.bson.Document;

import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static mtymes.common.time.DateUtil.toZonedDateTime;
import static mtymes.smartqueue.domain.JobId.jobId;
import static mtymes.smartqueue.domain.JobRequestId.jobRequestId;

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

    public String getString(String fieldName) {
        return getField(fieldName, false);
    }

    public ZonedDateTime getZonedDateTime(String fieldName) {
        Date value = getField(fieldName, false);
        return toZonedDateTime(value, DateUtil.UTC_ZONE_ID);
    }

    public DocListWrapper getList(String fieldName, boolean nullAllowed) {
        List value = getField(fieldName, nullAllowed);
        if (value == null) {
            return DocListWrapper.wrap(emptyList());
        } else {
            return DocListWrapper.wrap(value);
        }
    }

    public DocListWrapper getList(String fieldName) {
        List value = getField(fieldName, false);
        return DocListWrapper.wrap(value);
    }

    public JobRequestId getJobRequestId(String fieldName) {
        return jobRequestId(getString(fieldName));
    }

    public JobId getJobId(String fieldName) {
        return jobId(getString(fieldName));
    }

    public JobRequestState getJobRequestState(String fieldName) {
        return JobRequestState.valueOf(getString(fieldName));
    }

    public JobState getJobState(String fieldName) {
        return JobState.valueOf(getString(fieldName));
    }

    private <T> T getField(String fieldName, boolean nullAllowed) {
        T value = (T) doc.get(fieldName);
        if (value == null && !nullAllowed) {
            throw new NullPointerException(format("%s can't be null", fieldName));
        }
        return value;
    }
}
