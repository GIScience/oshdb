package org.heigit.ohsome.oshdb.util.time;

import java.time.Duration;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.chrono.ChronoZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that helps one manage a list of regularly spaced timestamps.
 */
public class OSHDBTimestamps implements OSHDBTimestampList {
  private static final Logger LOG = LoggerFactory.getLogger(OSHDBTimestamps.class);

  /**
   * Very basic time interval helper enumeration, to make usage of ISO 8601 time periods/intervals
   * more easy by providing aliases of the very most used intervals ("daily", "monthly", "yearly").
   */
  public enum Interval {
    YEARLY("P1Y"),
    QUARTERLY("P3M"),
    MONTHLY("P1M"),
    WEEKLY("P1W"),
    DAILY("P1D"),
    HOURLY("PT1H");

    final String value;
    Interval(final String value) {
      this.value = value;
    }
  }

  private final ZonedDateTime start;
  private final ZonedDateTime end;
  private final String period;
  private final boolean fromEnd;

  /**
   * Creates regularly spaced timestamps between a start and end date by time intervals defined
   * by an ISO 8601 "period" identifier.
   *
   * <p>
   * If fromEnd is true, the timestamps are computed starting with the end date, otherwise starting
   * with the start date of the interval.
   * </p>
   *
   * @param isoDateStart ISO 8601 date string representing the start date
   * @param isoDateEnd ISO 8601 date string representing the start date
   * @param isoPeriod ISO 8601 time period string representing the interval between the generated
   *                  timestamps
   * @param fromEnd computation of the timestamps starting with the end date of the interval
   *                (instead of the start date)
   */
  public OSHDBTimestamps(String isoDateStart, String isoDateEnd, String isoPeriod, boolean fromEnd
  ) {
    this.start = IsoDateTimeParser.parseIsoDateTime(isoDateStart);
    this.end = IsoDateTimeParser.parseIsoDateTime(isoDateEnd);
    this.period = isoPeriod;
    this.fromEnd = fromEnd;

    //validate start and end. start should be before end.
    if (start.isAfter(end)) {
      LOG.error("Start is after end, but must be earlier. start:{}, end: {}", start, end);
      throw new OSHDBTimestampIllegalArgumentException(String.format(
          "Start is after end, but must be earlier. start:%s, end: %s", start, end));
    }
  }

  /**
   * Creates regularly spaced timestamps between a start and end date by time intervals defined
   * by an ISO 8601 "period" identifier.
   *
   * <p>
   * The timestamps are computed starting with the start date.
   * </p>
   *
   * @param isoDateStart ISO 8601 date string representing the start date
   * @param isoDateEnd ISO 8601 date string representing the start date
   * @param isoPeriod ISO 8601 time period string representing the interval between the generated
   *                  timestamps
   */
  public OSHDBTimestamps(String isoDateStart, String isoDateEnd, String isoPeriod) {
    this(isoDateStart, isoDateEnd, isoPeriod, false);
  }

  /**
   * Creates regularly spaced timestamps between a start and end date by predefined time intervals.
   *
   * <p>
   * The timestamps are computed starting with the start date.
   * </p>
   *
   * @param isoDateStart ISO 8601 date string representing the start date
   * @param isoDateEnd ISO 8601 date string representing the start date
   * @param interval interval between the generated timestamps
   */
  public OSHDBTimestamps(String isoDateStart, String isoDateEnd, Interval interval) {
    this(isoDateStart, isoDateEnd, interval.value);
  }

  /**
   * Creates regularly spaced timestamps between a start and end date by predefined time intervals.
   *
   * <p>
   * If fromEnd is true, the timestamps are computed starting with the end date, otherwise
   * starting with the start date of the interval.
   * </p>
   *
   * @param isoDateStart ISO 8601 date string representing the start date
   * @param isoDateEnd ISO 8601 date string representing the start date
   * @param interval interval between the generated timestamps
   * @param fromEnd computation of the timestamps starting with the end date of the interval
   *                (instead of the start date)
   */
  public OSHDBTimestamps(String isoDateStart, String isoDateEnd, Interval interval, boolean fromEnd
  ) {
    this(isoDateStart, isoDateEnd, interval.value, fromEnd);
  }

  /**
   * Creates a "list" of two timestamps (consisting of a start and an end date).
   *
   * <p>
   * This can be used when one just wants to specify a single time interval without intermediate
   * timestamps.
   * </p>
   *
   * @param isoDateStart ISO 8601 date string representing the start date
   * @param isoDateEnd ISO 8601 date string representing the start date
   */
  public OSHDBTimestamps(String isoDateStart, String isoDateEnd) {
    this(isoDateStart, isoDateEnd, (String) null);
  }

  /**
   * Creates a "list" of a single timestamps (consisting of a only one specific date).
   *
   * <p>
   * This can be used when one just wants to specify a single time snapshot.
   * </p>
   *
   * @param isoDate ISO 8601 date string representing the date
   */
  public OSHDBTimestamps(String isoDate) {
    this(isoDate, isoDate);
  }

  /**
   * Provides the sorted list of (unix) timestamps representing this object's start/end date and
   * interval.
   *
   * @return a list of unix timestamps (measured in seconds)
   */
  @Override
  public TreeSet<OSHDBTimestamp> get() {
    Stream<ZonedDateTime> times;
    if (period != null) {
      times = getTimestampsAsEpochSeconds(start, end, period, fromEnd).stream();
    } else {
      if (start.equals(end)) {
        times = Stream.of(start);
      } else {
        times = Stream.of(start, end);
      }
    }
    return times
        .map(ChronoZonedDateTime::toEpochSecond)
        .map(OSHDBTimestamp::new)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  private static List<ZonedDateTime> getTimestampsAsEpochSeconds(
      ZonedDateTime start, ZonedDateTime end, String isoStringPeriod, boolean fromEnd
  ) {
    Map<String, Object> steps = IsoDateTimeParser.parseIsoPeriod(isoStringPeriod);

    Period period = (Period) steps.get("period");
    Duration duration = (Duration) steps.get("duration");

    List<ZonedDateTime> timestamps = new ArrayList<>();

    if (fromEnd) {
      ZonedDateTime currentTimestamp = ZonedDateTime.from(end);
      long startTimestamp = start.toEpochSecond();

      for (int counter = 1; currentTimestamp.toEpochSecond() >= startTimestamp; counter++) {
        timestamps.add(currentTimestamp);
        currentTimestamp = end
            .minus(period.multipliedBy(counter))
            .minus(duration.multipliedBy(counter));
      }
    } else {
      ZonedDateTime currentTimestamp = ZonedDateTime.from(start);
      long endTimestamp = end.toEpochSecond();

      for (int counter = 1; currentTimestamp.toEpochSecond() <= endTimestamp; counter++) {
        timestamps.add(currentTimestamp);
        currentTimestamp = start
            .plus(period.multipliedBy(counter))
            .plus(duration.multipliedBy(counter));
      }
    }

    return timestamps;
  }
}
