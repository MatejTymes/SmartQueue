package mtymes.smartqueue.dao;

import mtymes.smartqueue.domain.TaskId;
import org.junit.Test;

import java.time.Duration;

import static mtymes.smartqueue.domain.TaskConfigBuilder.taskConfigBuilder;
import static mtymes.test.Random.randomInt;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public abstract class BaseTaskTTLTest extends TaskHandlingTest {

    /* =========== */
    /* --- ttl --- */
    /* =========== */

    @Test
    public void shouldNotDeleteTaskWithoutTTL() {
        Duration ttl = Duration.ofSeconds(randomInt(2, 4));

        TaskId taskId = submitTask(taskConfigBuilder().build());

        // When
        waitFor(ttl.plusMinutes(1).plusSeconds(10));

        // Then
        assertThat(doesTaskExist(taskId), is(true));
    }

    @Test
    public void shouldSubmitTaskWithTTL() throws InterruptedException {
        Duration ttl = Duration.ofSeconds(randomInt(2, 4));

        // When
        TaskId taskId = submitTask(taskConfigBuilder()
                .ttl(ttl)
                .build());
        // Then
        assertThat(doesTaskExist(taskId), is(true));

        // When
//        waitFor(ttl.plusMinutes(2).plusSeconds(10));
        waitFor(ttl.plusMinutes(1).plusSeconds(10));
//        waitFor(ttl.plusSeconds(10));
        // Then
        assertThat(doesTaskExist(taskId), is(false));
    }

    @Test
    public void shouldCountTTLFromTheMomentItWasSet() throws InterruptedException {
        Duration ttl = Duration.ofSeconds(randomInt(2, 4));

        TaskId taskId = submitTask(taskConfigBuilder().build());

        waitFor(ttl.plusMinutes(1).plusSeconds(10));
        setTtl(taskId, ttl);
        assertThat(doesTaskExist(taskId), is(true));

        waitFor(ttl.plusMinutes(1).plusSeconds(10));
        assertThat(doesTaskExist(taskId), is(false));
    }
}
