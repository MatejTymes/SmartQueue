package mtymes.common.mongo;

import javafixes.math.Decimal;
import javafixes.object.Microtype;
import mtymes.common.time.DateUtil;
import org.bson.Document;
import org.bson.types.Decimal128;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.Maps.newLinkedHashMap;
import static java.util.stream.Collectors.toList;
import static javafixes.common.StreamUtil.toStream;

public class DocBuilder {

    private final Map<String, Object> values = newLinkedHashMap();

    public static DocBuilder docBuilder() {
        return new DocBuilder();
    }

    public static Document doc(String key, Object value) {
        return docBuilder().put(key, value).build();
    }

    public static Document emptyDoc() {
        return docBuilder().build();
    }

    public Document build() {
        return new Document(values);
    }

    public DocBuilder put(String key, Object value) {
        if (value instanceof Optional) {
            ((Optional) value).ifPresent(o -> values.put(key, toValueToStore(o)));
        } else {
            values.put(key, toValueToStore(value));
        }
        return this;
    }

    private Object toValueToStore(Object value) {
        Object valueToStore = value;

        if (valueToStore instanceof Microtype) {
            valueToStore = ((Microtype) valueToStore).getValue();
        }

        if (valueToStore instanceof Decimal) {
            valueToStore = new Decimal128(((Decimal) valueToStore).bigDecimalValue());
        } else if (valueToStore instanceof UUID) {
            valueToStore = valueToStore.toString();
        } else if (valueToStore instanceof ZonedDateTime) {
            valueToStore = DateUtil.toDate((ZonedDateTime) valueToStore);
        } else if (valueToStore instanceof Enum) {
            valueToStore = ((Enum) valueToStore).name();
        } else if (valueToStore instanceof Iterable) {
            valueToStore = toStream((Iterable) valueToStore)
                    .map(this::toValueToStore)
                    .collect(toList());
        }

        return valueToStore;
    }
}
