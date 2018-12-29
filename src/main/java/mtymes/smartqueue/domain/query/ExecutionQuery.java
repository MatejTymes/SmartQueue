package mtymes.smartqueue.domain.query;

import javafixes.object.DataObject;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static javafixes.common.CollectionUtil.newList;

public class ExecutionQuery extends DataObject {

    private static final ExecutionQuery EMPTY_QUERY = new ExecutionQuery(emptyList());

    public final Iterable<String> groups;

    public ExecutionQuery(Iterable<String> groups) {
        this.groups = unmodifiableList(newList(groups));
    }

    public static ExecutionQuery emptyQuery() {
        return EMPTY_QUERY;
    }
}
