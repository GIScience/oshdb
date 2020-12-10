package org.heigit.ohsome.oshdb.util.time;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTimeoutException;

public class IsoDateTimeParser {
  private static final ZoneId UTC = ZoneId.of("Z");

  private IsoDateTimeParser() {
    throw new IllegalStateException("utility class");
  }

  /**
   * Converts an ISO 8601 Date or combined Date-Time String into a UTC based ZonedDateTime Object.
   *
   * <p>No other time zones are supported, please provide your date-time in UTC
   * with or without trailing "Z".
   *
   * <p>Time zone designators in the form "+hh:mm" are not accepted.
   *
   * <p>Examples:
   * <pre>
   *  ISO Date: 2017
   *  ISO Date: 2016-03
   *  ISO Date: 2015-06-05
   *  Basic ISO Date: 20170305
   *  combined: 2011-10-03T20[Z]
   *  combined: 2011-10-03T20:15[Z]
   *  combined: 2011-10-03T20:15:25[Z]
   *  combined: 2011-10-03T20:15:25.123[Z]
   * </pre>
   *
   * @param isoDateTime ISO Date or ISO DateTime string
   * @return ZonedDateTime
   * @throws OSHDBTimestampException if date or datetime pattern is not supported
   * @throws java.time.DateTimeException if date or datetime cannot be parsed
   */
  public static ZonedDateTime parseIsoDateTime(String isoDateTime) {
    isoDateTime = isoDateTime.trim();

    if (isoDateTime.startsWith("-")) {
      throw new OSHDBTimestampIllegalArgumentException("Negative Dates are not supported: "
          + isoDateTime);
    }

    if (isoDateTime.matches("^([0-9]|-)*T([0-9]|:|\\.)*[+-]([0-9]|:)*$")) {
      throw new OSHDBTimestampIllegalArgumentException(
          "No timezone designator other than 'Z' is allowed: " + isoDateTime);
    }

    // always remove trailing Z, no other timezones are allowed anyways
    if (isoDateTime.endsWith("Z")) {
      isoDateTime = isoDateTime.substring(0, isoDateTime.length() - 1);
    }

    switch (isoDateTime.length()) {
      case 4:
        // pattern "uuuu"
        return Year.parse(isoDateTime).atDay(1).atStartOfDay(UTC);
      case 6:
        // pattern "uuuuMM"
        return LocalDate.parse(isoDateTime + "01", DateTimeFormatter.BASIC_ISO_DATE)
            .atStartOfDay(UTC);
      case 7:
        // pattern "uuuu-MM"
        return YearMonth.parse(isoDateTime).atDay(1).atStartOfDay(UTC);
      case 8:
        // pattern "uuuuMMDD"
        return LocalDate.parse(isoDateTime, DateTimeFormatter.BASIC_ISO_DATE).atStartOfDay(UTC);
      case 10:
        // pattern "uuuu-MM-DD"
        return LocalDate.parse(isoDateTime, DateTimeFormatter.ISO_DATE).atStartOfDay(UTC);
      //with time
      case 13:
        // pattern "uuuu-MM-DDTHH"
        return LocalDateTime.parse(isoDateTime + ":00:00Z", DateTimeFormatter.ISO_DATE_TIME)
            .atZone(UTC);
      case 16:
        // pattern "uuuu-MM-DDTHH:MM"
        return LocalDateTime.parse(isoDateTime + ":00Z", DateTimeFormatter.ISO_DATE_TIME)
            .atZone(UTC);
      case 19:
        // pattern "uuuu-MM-DDTHH:MM:ss"
      case 23:
        // pattern "uuuu-MM-DDTHH:MM:ss.sss"
        return LocalDateTime.parse(isoDateTime + "Z", DateTimeFormatter.ISO_DATE_TIME).atZone(UTC);
      default:
        throw new OSHDBTimestampIllegalArgumentException("Date or DateTime Format not supported: "
            + isoDateTime);
    }
  }

  /**
   * Converts an ISO Period string into two parts,
   * a period for the date part and a duration for the time part.
   *
   * <p>Examples:
   * <pre>
   *  ISO Date Period: P1Y (years)
   *  ISO Date Period: P3M (months)
   *  ISO Date Period: P1D (days)
   *  mixed ISO Dates:  P1Y25D or 1M15D
   *  ISO Date Period: P2W (weeks)
   *  ISO Time Duration: PT1H (hours)
   *  ISO Time Duration: PT30M (minutes)
   *  ISO Time Duration: PT20S (seconds)
   *  combined DateTime: P1Y3M5DT5H30M20S
   * </pre>
   *
   * @param isoPeriodString ISO Period string
   * @return HashMap with a Period object and a Duration object
   * @throws OSHDBTimeoutException if the isoPeriodString is not valid
   */
  public static Map<String, Object> parseIsoPeriod(String isoPeriodString) {
    Period period = Period.ZERO;
    Duration duration = Duration.ZERO;

    //  isoPeriod should follow the ISO 8601 period notation without startdate
    // P ^= Period
    // Y =  Years
    // M =  Month
    // W =  Weeks
    // D =  Days
    // T ^= Time part starts here
    // H = Hours
    // M = Minutes
    // S = Seconds

    // Examples:
    //    Full DateTime Period: PnYnMnDTnHnMnS, e.g P1Y3M10DT1H15M25S
    //                              (1 year 3months 10 days 1 hour 15 minutes and 25 seconds)
    //    Full Date Period:     PnYnMnD, e.g. P1Y3M10D
    //                              (1 year 3 months and 10 days)
    //    Short Date Period:    e.g PnY or PnMnD or any combination of years, months and days
    //    Week Period:          PnW, e.g. P2W 2 weeks
    //    Full Time Duration:   PTnHnMnS, e.g. PT1H3M25S (1 hour 3 minutes and 25 seconds)
    //    Short Time Duration:  PTnH or any combination of hours, minutes and seconds
    //                              (PT1H10S 1 hour and 10 seconds)

    // validate period
    final Pattern isValidPattern = Pattern.compile(
        "^P(?!$)(\\d+Y)?(\\d+M)?(\\d+W)?(\\d+D)?(T(?=\\d+[HMS])(\\d+H)?(\\d+M)?(\\d+S)?)?$",
        Pattern.MULTILINE);

    if (!isValidPattern.matcher(isoPeriodString).matches()) {
      throw new OSHDBTimestampParseException("isoPeriodString is not valid: "
          + isoPeriodString
          + " (Format: P[yY][mM][dD][T[hH][mM][sS]])");
    }

    boolean hasDateAndTimeComponent = Pattern.matches("P.+T.+", isoPeriodString);
    boolean hasOnlyTimeComponent = isoPeriodString.startsWith("PT");

    if (hasDateAndTimeComponent) {
      String[] periodParts = isoPeriodString.split("T");
      period = Period.parse(periodParts[0]);
      duration = Duration.parse("PT" + periodParts[1]);
    } else if (hasOnlyTimeComponent) {
      duration = Duration.parse(isoPeriodString);
    } else { //hasOnlyDateComponent
      period = Period.parse(isoPeriodString);
    }

    // check against zero length period/duration
    if (duration.equals(Duration.ZERO) && period.equals(Period.ZERO)) {
      throw new OSHDBTimestampIllegalArgumentException("the specified period has a length of ZERO: "
          + isoPeriodString);
    }

    Map<String, Object> result = new HashMap<>();
    result.put("period", period);
    result.put("duration", duration);

    return result;
  }
}
