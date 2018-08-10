package mtymes.common.time;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

public class DateUtil {

    public static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

    // todo: mtymes - test this
    public static Date toDate(ZonedDateTime dateTime) {
        return Date.from(dateTime.withZoneSameInstant(UTC_ZONE_ID).toInstant());
    }

    // todo: mtymes - test this
    public static ZonedDateTime toZonedDateTime(Date date, ZoneId zoneId) {
        return ZonedDateTime.ofInstant(
                date.toInstant(),
                zoneId
        );
    }
}
