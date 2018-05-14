package org.heigit.bigspatialdata.oshdb.util.time;

import java.time.Duration;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.*;

import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that helps one manage a list of regularly spaced timestamps.
 */
public class OSHDBTimestamps implements OSHDBTimestampList {
  private static final Logger LOG = LoggerFactory.getLogger(OSHDBTimestamps.class);

  /**
   * Very basic time interval helper enumeration, to make usage of ISO 8601 time periods/intervals more easy
   * by providing aliases of the very most used intervals ("daily", "monthly", "yearly").
   */
  public enum Interval {
    YEARLY("P1Y"),
    MONTHLY("P1M"),
    DAILY("P1D");

    final String value;
    Interval(final String value) {
      this.value = value;
    }
  }

  private String start;
  private String end;
  private String period;
  private Boolean fromEnd;

  /**
   * Creates regularly spaced timestamps between a start and end date by time intervals defined by an ISO 8601 "period" identifier.
   *
   * The timestamps are computed starting with the start date.
   *
   * @param isoDateStart ISO 8601 date string representing the start date
   * @param isoDateEnd ISO 8601 date string representing the start date
   * @param isoPeriod ISO 8601 time period string representing the interval between the generated timestamps
   */
  public OSHDBTimestamps(String isoDateStart, String isoDateEnd, String isoPeriod) {
    this(isoDateStart, isoDateEnd, isoPeriod, false);
  }

  /**
   * Creates regularly spaced timestamps between a start and end date by time intervals defined by an ISO 8601 "period" identifier.
   *
   * If fromEnd is true, the timestamps are computed starting with the end date, otherwise starting with the start
   * date of the interval.
   *
   * @param isoDateStart ISO 8601 date string representing the start date
   * @param isoDateEnd ISO 8601 date string representing the start date
   * @param isoPeriod ISO 8601 time period string representing the interval between the generated timestamps
   * @param fromEnd computation of the timestamps starting with the end date of the interval (instead of the start date)
   */
  public OSHDBTimestamps(String isoDateStart, String isoDateEnd, String isoPeriod, Boolean fromEnd) {
    this.start = isoDateStart;
    this.end = isoDateEnd;
    this.period = isoPeriod;
    this.fromEnd = fromEnd;
  }

  /**
   * Creates regularly spaced timestamps between a start and end date by predefined time intervals.
   *
   * The timestamps are computed starting with the start date.
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
   * If fromEnd is true, the timestamps are computed starting with the end date, otherwise starting with the start
   * date of the interval.
   *
   * @param isoDateStart ISO 8601 date string representing the start date
   * @param isoDateEnd ISO 8601 date string representing the start date
   * @param interval interval between the generated timestamps
   * @param fromEnd computation of the timestamps starting with the end date of the interval (instead of the start date)
   */
  public OSHDBTimestamps(String isoDateStart, String isoDateEnd, Interval interval, Boolean fromEnd) {
    this(isoDateStart, isoDateEnd, interval.value, fromEnd);
  }

  /**
   * Creates a "list" of two timestamps (consisting of a start and an end date).
   *
   * This can be used when one just wants to specify a single time interval without intermediate timestamps.
   *
   * @param isoDateStart ISO 8601 date string representing the start date
   * @param isoDateEnd ISO 8601 date string representing the start date
   */
  public OSHDBTimestamps(String isoDateStart, String isoDateEnd) {
    this.start = isoDateStart;
    this.end = isoDateEnd;
    this.period = null;
  }

  /**
   * Creates a "list" of a single timestamps (consisting of a only one specific date).
   *
   * This can be used when one just wants to specify a single time snapshot.
   *
   * @param isoDate ISO 8601 date string representing the date
   */
  public OSHDBTimestamps(String isoDate) {
    this.start = isoDate;
    this.end = isoDate;
    this.period = null;
  }

  /**
   * Provides the sorted list of (unix) timestamps representing this object's start/end date and interval.
   *
   * @return a list of unix timestamps (measured in seconds)
   */
  public SortedSet<OSHDBTimestamp> get() {
    if (period != null) {
      return new TreeSet<>(
          _getTimestampsAsEpochSeconds(start, end, period, fromEnd).stream().map(OSHDBTimestamp::new).collect(Collectors.toList())
      );
    } else {
      SortedSet<OSHDBTimestamp> timestamps = new TreeSet<>();
      try {
        timestamps.add(new OSHDBTimestamp(ISODateTimeParser.parseISODateTime(start).toEpochSecond()));
        if (start.equals(end)) return timestamps;
        timestamps.add(new OSHDBTimestamp(ISODateTimeParser.parseISODateTime(end).toEpochSecond()));
      } catch (Exception e) {
        LOG.error("Unable to parse timestamp string, skipping: " + e.getMessage());
      }
      return timestamps;
    }
  }

  public OSHDBTimestamp getEnd() throws Exception {
    return new OSHDBTimestamp(ISODateTimeParser.parseISODateTime(this.end).toEpochSecond());
  }

  private static List<Long> _getTimestampsAsEpochSeconds(String isoStringStart, String isoStringEnd, String isoStringPeriod, Boolean fromEnd) {
    try {
      ZonedDateTime start = ISODateTimeParser.parseISODateTime(isoStringStart);
      ZonedDateTime end = ISODateTimeParser.parseISODateTime(isoStringEnd);
      Map<String, Object> steps = ISODateTimeParser.parseISOPeriod(isoStringPeriod);

      Period period = (Period) steps.get("period");
      Duration duration = (Duration) steps.get("duration");

      //validate start and end. start should be before end.
      if (start.isAfter(end)) {
        LOG.error("Start is after end, but must be earlier: \n" + "start: " + start + "\nend: " + end);
        return Collections.emptyList();
      }

      List<Long> timestamps = new ArrayList<>();

      if (fromEnd) {
        ZonedDateTime currentTimestamp = ZonedDateTime.from(end);
        long startTimestamp = start.toEpochSecond();

        while (currentTimestamp.toEpochSecond() >= startTimestamp) {
          timestamps.add(currentTimestamp.toEpochSecond());
          currentTimestamp = currentTimestamp.minus(period).minus(duration);
        }
      } else {
        ZonedDateTime currentTimestamp = ZonedDateTime.from(start);
        long endTimestamp = end.toEpochSecond();

        while (currentTimestamp.toEpochSecond() <= endTimestamp) {
          timestamps.add(currentTimestamp.toEpochSecond());
          currentTimestamp = currentTimestamp.plus(period).plus(duration);
        }
      }

      return timestamps;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return Collections.emptyList();
    }
  }
}
