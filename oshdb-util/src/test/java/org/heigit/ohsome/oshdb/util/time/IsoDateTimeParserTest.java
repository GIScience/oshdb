package org.heigit.ohsome.oshdb.util.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Period;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link IsoDateTimeParser} class.
 */
class IsoDateTimeParserTest {

  @Test
  void testParseIsoDateTime() {
    // test allowed variants

    //Basic Dates
    String[] yyyy = {"2020-01-01T00:00Z", "2020"};
    String[] yyyymm = {"2020-02-01T00:00Z", "202002"};
    String[] yyyymmdd = {"2020-02-17T00:00Z", "20200217"};

    assertEquals(yyyy[0], IsoDateTimeParser.parseIsoDateTime(yyyy[1]).toString());
    assertEquals(yyyymm[0], IsoDateTimeParser.parseIsoDateTime(yyyymm[1]).toString());
    assertEquals(yyyymmdd[0], IsoDateTimeParser.parseIsoDateTime(yyyymmdd[1]).toString());

    //Extended Dates
    String[] yyyyMm = {"2020-02-01T00:00Z", "2020-02"};
    String[] yyyyMmDd = {"2020-02-17T00:00Z", "2020-02-17"};

    assertEquals(yyyyMm[0], IsoDateTimeParser.parseIsoDateTime(yyyyMm[1]).toString());
    assertEquals(yyyyMmDd[0], IsoDateTimeParser.parseIsoDateTime(yyyyMmDd[1]).toString());

    //Extended Date-Time
    String[] yyyyMmDdHh = {"2020-02-17T23:00Z", "2020-02-17T23"};
    String[] yyyyMmDdHhz = {"2020-02-17T23:00Z", "2020-02-17T23Z"};

    String[] yyyyMmDdHhMm = {"2020-02-17T23:55Z", "2020-02-17T23:55"};
    String[] yyyyMmDdHhMmz = {"2020-02-17T23:55Z", "2020-02-17T23:55Z"};

    String[] yyyyMmDdHhMmSs = {"2020-02-17T23:55:12Z", "2020-02-17T23:55:12"};
    String[] yyyyMmDdHhMmSsz = {"2020-02-17T23:55:12Z", "2020-02-17T23:55:12Z"};

    String[] yyyyMmDdHhMmSsSss = {"2020-02-17T23:55:12.999Z", "2020-02-17T23:55:12.999"};
    String[] yyyyMmDdHhMmSsSssz = {"2020-02-17T23:55:12.999Z", "2020-02-17T23:55:12.999Z"};

    assertEquals(yyyyMmDdHh[0],
        IsoDateTimeParser.parseIsoDateTime(yyyyMmDdHh[1]).toString());
    assertEquals(yyyyMmDdHhz[0],
        IsoDateTimeParser.parseIsoDateTime(yyyyMmDdHhz[1]).toString());

    assertEquals(yyyyMmDdHhMm[0],
        IsoDateTimeParser.parseIsoDateTime(yyyyMmDdHhMm[1]).toString());
    assertEquals(yyyyMmDdHhMmz[0],
        IsoDateTimeParser.parseIsoDateTime(yyyyMmDdHhMmz[1]).toString());

    assertEquals(yyyyMmDdHhMmSs[0],
        IsoDateTimeParser.parseIsoDateTime(yyyyMmDdHhMmSs[1]).toString());
    assertEquals(yyyyMmDdHhMmSsz[0],
        IsoDateTimeParser.parseIsoDateTime(yyyyMmDdHhMmSsz[1]).toString());

    assertEquals(yyyyMmDdHhMmSsSss[0],
        IsoDateTimeParser.parseIsoDateTime(yyyyMmDdHhMmSsSss[1]).toString());
    assertEquals(yyyyMmDdHhMmSsSssz[0],
        IsoDateTimeParser.parseIsoDateTime(yyyyMmDdHhMmSsSssz[1]).toString());

  }

  @Test()
  void throwsNegativeDateParseIsoDateTime() {
    //Negative Dates
    String nyyyy = "-0333";
    assertThrows(OSHDBTimestampException.class, () -> {
      IsoDateTimeParser.parseIsoDateTime(nyyyy);
    });
  }

  @Test()
  void throwsShortYearParseIsoDateTime() {
    //Short Year
    String yy = "12";
    assertThrows(OSHDBTimestampException.class, () -> {
      IsoDateTimeParser.parseIsoDateTime(yy);
    });
  }

  @Test()
  void throwsPosTimezoneHhParseIsoDateTime() {
    String posTimezoneHh = "2020-02-17T23:55+02";
    assertThrows(OSHDBTimestampException.class, () -> {
      IsoDateTimeParser.parseIsoDateTime(posTimezoneHh);
    });
  }

  @Test()
  void throwsPosTimezoneHhMmParseIsoDateTime() {
    String posTimezoneHhmm = "2020-02-17T23:55+0230";
    assertThrows(OSHDBTimestampException.class, () -> {
      IsoDateTimeParser.parseIsoDateTime(posTimezoneHhmm);
    });
  }

  @Test()
  void throwsPosTimezoneHh_MmParseIsoDateTime() {
    String posTimezoneHhMm = "2020-02-17T23:55+02:30";
    assertThrows(OSHDBTimestampException.class, () -> {
      IsoDateTimeParser.parseIsoDateTime(posTimezoneHhMm);
    });
  }

  @Test()
  void throwsNegTimezoneHhParseIsoDateTime() {
    String negTimezoneHh = "2020-02-17T23:55-02";
    assertThrows(OSHDBTimestampException.class, () -> {
      IsoDateTimeParser.parseIsoDateTime(negTimezoneHh);
    });
  }

  @Test()
  void throwsNegTimezoneHhMmParseIsoDateTime() {
    String negTimezoneHhMm = "2020-02-17T23:55-0230";
    assertThrows(OSHDBTimestampException.class, () -> {
      IsoDateTimeParser.parseIsoDateTime(negTimezoneHhMm);
    });
  }

  @Test()
  void throwsNegTimezoneHh_MmParseIsoDateTime() {
    String negTimezoneHhMm = "2020-02-17T23:55-02:30";
    assertThrows(OSHDBTimestampException.class, () -> {
      IsoDateTimeParser.parseIsoDateTime(negTimezoneHhMm);
    });
  }

  @Test()
  void throwsWrongDateParseIsoDateTime() {
    //Wrong Date
    String wrongDateTime = "2020-13-01T00:00";
    assertThrows(DateTimeException.class, () -> {
      IsoDateTimeParser.parseIsoDateTime(wrongDateTime);
    });
  }


  @Test
  void testParseIsoPeriod() {
    // test allowed variants

    //    Full DateTime Period: PnYnMnDTnHnMnS,
    //      e.g P1Y3M10DT1H15M25S (1 year 3months 10 days 1 hour 15 minutes and 25 seconds)
    //    Full Date Period:     PnYnMnD,
    //      e.g. P1Y3M10D (1 year 3 months and 10 days)
    //    Short Date Period:
    //      e.g PnY or PnMnD or any combination of years, months and days
    //    Week Period:           PnW,
    //      e.g. P2W (2 weeks)
    //    Full Time Duration:   PTnHnMnS,
    //      e.g. PT1H3M25S (1 hour 3 minutes and 25 seconds)
    //    Short Time Duration:  PTnH or any combination of hours, minutes and seconds
    //      e.g. PT1H10S (1 hour and 10 seconds)

    // input, output Period, output Duration
    String[] fullDateTimePeriod = {"P1Y3M10DT1H15M25S", "P1Y3M10D", "PT1H15M25S"};
    Map<String, Object> fullYearMonthDayTime =
        IsoDateTimeParser.parseIsoPeriod(fullDateTimePeriod[0]);
    Period fullYearMonthDayTimePeriod = (Period) fullYearMonthDayTime.get("period");
    Duration fullYearMonthDayTimeDuration = (Duration) fullYearMonthDayTime.get("duration");
    assertEquals(fullDateTimePeriod[1], fullYearMonthDayTimePeriod.toString());
    assertEquals(fullDateTimePeriod[2], fullYearMonthDayTimeDuration.toString());

    // Period output should be same as input, Duration should be ZERO
    String fullDatePeriod = "P1Y3M10D";
    Map<String, Object> fullYearMonthDay = IsoDateTimeParser.parseIsoPeriod(fullDatePeriod);
    Period fullYearMonthDayPeriod = (Period) fullYearMonthDay.get("period");
    Duration fullYearMonthDayDuration = (Duration) fullYearMonthDay.get("duration");
    assertEquals(fullDatePeriod, fullYearMonthDayPeriod.toString());
    assertTrue(fullYearMonthDayDuration.isZero());

    // Period output should be same as input, Duration should be ZERO
    String shortDatePeriod = "P3M10D";
    Map<String, Object> shortMonthDay = IsoDateTimeParser.parseIsoPeriod(shortDatePeriod);
    Period shortMonthDayPeriod = (Period) shortMonthDay.get("period");
    Duration shortMonthDayDuration = (Duration) shortMonthDay.get("duration");
    assertEquals(shortDatePeriod, shortMonthDayPeriod.toString());
    assertTrue(shortMonthDayDuration.isZero());

    // Period should equal 14 days, Duration should be ZERO
    String weekPeriod = "P2W";
    Map<String, Object> twoWeeks = IsoDateTimeParser.parseIsoPeriod(weekPeriod);
    Period twoWeeksPeriod = (Period) twoWeeks.get("period");
    Duration twoWeeksDuration = (Duration) twoWeeks.get("duration");
    assertEquals(14, twoWeeksPeriod.getDays());
    assertTrue(twoWeeksDuration.isZero());

  }

  @Test()
  void throwsFormatParseIsoPeriod() {
    assertThrows(OSHDBTimestampException.class, () -> {
      // test throw exeption for unsupported formats
      IsoDateTimeParser.parseIsoPeriod("PT1Y2M");
    });
  }

  @Test()
  void throwsZeroLengthParseIsoPeriod() {
    assertThrows(OSHDBTimestampException.class, () -> {
      //test for zero length ISOPeriod
      IsoDateTimeParser.parseIsoPeriod("PT0S");
    });
  }

}
