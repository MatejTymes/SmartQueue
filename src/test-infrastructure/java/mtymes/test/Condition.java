package mtymes.test;


import java.util.Set;
import java.util.function.Function;

import static javafixes.common.CollectionUtil.newSet;

public interface Condition<T> extends Function<T, Boolean> {

    @SafeVarargs
    static <T> Condition<T> otherThan(T... values) {
        Set<T> exclusions = newSet(values);
        return value -> !exclusions.contains(value);
    }
}
