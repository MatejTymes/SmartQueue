package mtymes.test;

import mtymes.smartqueue.domain.JobId;
import mtymes.smartqueue.domain.JobRequestId;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;

import static mtymes.smartqueue.domain.JobId.jobId;
import static mtymes.smartqueue.domain.JobRequestId.jobRequestId;

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

    public static JobRequestId randomJobRequestId() {
        return jobRequestId(randomUUID());
    }

    public static JobId randomJobId() {
        return jobId(randomUUID());
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
