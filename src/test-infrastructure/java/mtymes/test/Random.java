package mtymes.test;

import mtymes.smartqueue.domain.TaskGroup;
import mtymes.smartqueue.domain.RunId;
import mtymes.smartqueue.domain.TaskId;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;

public class Random {

    @SafeVarargs
    public static int randomInt(int from, int to, Condition<Integer>... validityConditions) {
        return generateValidValue(
                // typecast it to long as otherwise we could get int overflow
                () -> (int) ((long) (Math.random() * ((long) to - (long) from + 1L)) + (long) from),
                validityConditions
        );
    }

    @SafeVarargs
    public static long randomLong(long from, long to, Condition<Long>... validityConditions) {
        return generateValidValue(
                () -> ThreadLocalRandom.current().nextLong(from, to) + (long) randomInt(0, 1),
                validityConditions
        );
    }

    public static UUID randomUUID() {
        return UUID.randomUUID();
    }

    public static Duration randomMillis() {
        return Duration.ofMillis(randomLong(0, 100_000));
    }

    public static TaskId randomTaskId() {
        return TaskId.taskId(randomUUID());
    }

    public static TaskGroup randomTaskGroup() {
        return TaskGroup.taskGroup("taskGroup-" + randomInt(0, 15_000));
    }

    public static RunId randomRunId() {
        return RunId.runId(randomUUID());
    }

    @SafeVarargs
    private static <T> T generateValidValue(Supplier<T> generator, Condition<T>... validityConditions) {
        T value;

        int infiniteCycleCounter = 0;

        boolean valid;
        do {
            valid = true;
            value = generator.get();
            for (Function<T, Boolean> validityCondition : validityConditions) {
                if (!validityCondition.apply(value)) {
                    valid = false;
                    break;
                }
            }

            if (infiniteCycleCounter++ == 1_000) {
                throw new IllegalStateException("Possibly reached infinite cycle - unable to generate value after 1000 attempts.");
            }
        } while (!valid);

        return value;
    }
}
