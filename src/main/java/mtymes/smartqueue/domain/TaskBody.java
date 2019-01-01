package mtymes.smartqueue.domain;

import javafixes.object.DataObject;

public class TaskBody extends DataObject {

    public final String content;

    public TaskBody(String content) {
        this.content = content;
    }
}
