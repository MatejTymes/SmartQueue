package mtymes.smartqueue.domain;

import java.time.Duration;
import java.util.Optional;

public class TaskConfigBuilder {

    private int attemptCount = 1;
    private Optional<Duration> ttl = Optional.empty();

    public static TaskConfigBuilder taskConfigBuilder() {
        return new TaskConfigBuilder();
    }

    public static TaskConfig taskConfig(int attemptCount) {
        return taskConfigBuilder().attemptCount(attemptCount).build();
    }

    public TaskConfig build() {
        return new TaskConfig(attemptCount, ttl);
    }

    public TaskConfigBuilder attemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
        return this;
    }

    public TaskConfigBuilder ttl(Duration ttl) {
        this.ttl = Optional.of(ttl);
        return this;
    }

    public TaskConfigBuilder noTTLLimit() {
        this.ttl = Optional.empty();
        return this;
    }
}
