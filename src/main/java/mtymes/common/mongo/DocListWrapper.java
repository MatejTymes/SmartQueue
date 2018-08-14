package mtymes.common.mongo;

import org.bson.Document;

import java.util.List;
import java.util.function.Function;

import static javafixes.common.CollectionUtil.newList;

// todo: mtymes - test this
public class DocListWrapper {

    private final List<?> list;

    public DocListWrapper(List list) {
        this.list = list;
    }

    public static DocListWrapper wrap(List<?> list) {
        return new DocListWrapper(list);
    }

    public <T> List<T> mapDoc(Function<DocWrapper, T> mapper) {
        List<T> results = newList();
        for (Object item : list) {
            DocWrapper wrapper = DocWrapper.wrap((Document) item);
            results.add(mapper.apply(wrapper));
        }
        return results;
    }

    public DocWrapper lastDoc() {
        return DocWrapper.wrap((Document) list.get(list.size() - 1));
    }
}
