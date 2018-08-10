package mtymes.common.time;

import java.time.ZonedDateTime;

import static mtymes.common.time.DateUtil.UTC_ZONE_ID;

// todo: test this
public class Clock {

    public ZonedDateTime now() {
        return ZonedDateTime.now(UTC_ZONE_ID);
    }
}
