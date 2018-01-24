package org.heigit.bigspatialdata.oshdb.util.time;

import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampList;
import org.heigit.bigspatialdata.oshdb.util.time.ISODateTimeParser;
import java.time.Duration;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.*;

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

  @Deprecated
  public OSHDBTimestamps(int startYear, int endYear, int startMonth, int endMonth, int startDay, int endDay) {
    super();
    this.start = "" + startYear + "-" + startMonth + "-" + startDay;
    this.end   = "" +   endYear + "-" +   endMonth + "-" +   endDay;
    this.period = "P1D";
  }

  @Deprecated
  public OSHDBTimestamps(int startYear, int endYear, int startMonth, int endMonth) {
    this(startYear, endYear, startMonth, endMonth, 1, 1);
    this.period = "P1M";
  }

  @Deprecated
  public OSHDBTimestamps(int startYear, int endYear) {
    this(startYear, endYear, 1,1, 1, 1);
    this.period = "P1Y";
  }

  /**
   * Creates regularly spaced timestamps between a start and end date by time intervals defined by an ISO 8601 "period" identifier.
   *
   * @param isoDateStart ISO 8601 date string representing the start date
   * @param isoDateEnd ISO 8601 date string representing the start date
   * @param isoPeriod ISO 8601 time period string representing the interval between the generated timestamps
   */
  public OSHDBTimestamps(String isoDateStart, String isoDateEnd, String isoPeriod) {
    this.start = isoDateStart;
    this.end = isoDateEnd;
    this.period = isoPeriod;
  }

  /**
   * Creates regularly spaced timestamps between a start and end date by predefined time intervals.
   *
   * @param isoDateStart ISO 8601 date string representing the start date
   * @param isoDateEnd ISO 8601 date string representing the start date
   * @param interval interval between the generated timestamps
   */
  public OSHDBTimestamps(String isoDateStart, String isoDateEnd, Interval interval) {
    this(isoDateStart, isoDateEnd, interval.value);
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
  public List<Long> getTimestamps() {
    if (period != null) {
      return _getTimestampsAsEpochSeconds(start, end, period);
    } else {
      List<Long> timestamps = new ArrayList<>(2);
      try {
        timestamps.add(ISODateTimeParser.parseISODateTime(start).toEpochSecond());
        if (!start.equals(end)) {
          timestamps.add(ISODateTimeParser.parseISODateTime(end).toEpochSecond());
        }
      } catch (Exception e) {
        LOG.error(e.getMessage());
        return Collections.emptyList();
      }
      return timestamps;
    }
  }

  private static List<Long> _getTimestampsAsEpochSeconds(String isoStringStart, String isoStringEnd, String isoStringPeriod) {
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

      ZonedDateTime currentTimestamp = ZonedDateTime.from(start);
      long endTimestamp = end.toEpochSecond();

      while (currentTimestamp.toEpochSecond() <= endTimestamp) {
        timestamps.add(currentTimestamp.toEpochSecond());
        currentTimestamp = currentTimestamp.plus(period).plus(duration);
      }

      return timestamps;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return Collections.emptyList();
    }
  }

  private static List<Long> _getTimestampsAsEpochSeconds(String isoStringStart, String isoStringPeriod, int numberOfTimestamps) {
    if (numberOfTimestamps < 1) {
      LOG.error("Specified less than one timestamp :" + numberOfTimestamps);
      return Collections.emptyList();
    }

    try {
      List<Long> timestamps = new ArrayList<>(numberOfTimestamps);

      ZonedDateTime start = ISODateTimeParser.parseISODateTime(isoStringStart);
      Map<String, Object> steps = ISODateTimeParser.parseISOPeriod(isoStringPeriod);
      Period period = (Period) steps.get("period");
      Duration duration = (Duration) steps.get("duration");

      int counter = 0;
      ZonedDateTime currentTimestamp = ZonedDateTime.from(start);

      while (counter < numberOfTimestamps) {
        timestamps.add(currentTimestamp.toEpochSecond());
        currentTimestamp = currentTimestamp.plus(period).plus(duration);
        counter++;
      }

      return timestamps;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return Collections.emptyList();
    }
  }

  @Deprecated
  public static List<Long> getTimestampsAsEpochSeconds(String isoStringStart, String isoStringEnd, String isoStringPeriod) {
    return _getTimestampsAsEpochSeconds(isoStringStart, isoStringEnd, isoStringPeriod);
  }
  @Deprecated
  public static List<Long> getTimestampsAsEpochSeconds(String isoStringStart, String isoStringPeriod, int numberOfTimestamps) {
    return _getTimestampsAsEpochSeconds(isoStringStart, isoStringPeriod, numberOfTimestamps);
  }
  
}
