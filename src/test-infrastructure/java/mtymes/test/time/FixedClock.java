package mtymes.test.time;

import mtymes.common.time.Clock;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import static mtymes.common.time.DateUtil.UTC_ZONE_ID;

public class FixedClock extends Clock {

    private volatile ZonedDateTime now;

    public FixedClock(ZonedDateTime now) {
        setNow(now);
    }

    public FixedClock() {
        this(ZonedDateTime.now(UTC_ZONE_ID));
    }

    @Override
    public ZonedDateTime now() {
        return now;
    }

    public ZonedDateTime setNow(ZonedDateTime now) {
        ZonedDateTime newTime = now.truncatedTo(ChronoUnit.MILLIS);
        this.now = newTime;
        return newTime;
    }

    public ZonedDateTime increaseBy(Duration duration) {
        return setNow(now.plus(duration));
    }

    public ZonedDateTime increaseBySeconds(long seconds) {
        return increaseBy(Duration.ofSeconds(seconds));
    }

    public ZonedDateTime increaseByMinutes(long minutes) {
        return increaseBy(Duration.ofMinutes(minutes));
    }

    public ZonedDateTime increaseByHours(long hours) {
        return increaseBy(Duration.ofHours(hours));
    }

    public ZonedDateTime increaseByDays(long days) {
        return increaseBy(Duration.ofDays(days));
    }
}
