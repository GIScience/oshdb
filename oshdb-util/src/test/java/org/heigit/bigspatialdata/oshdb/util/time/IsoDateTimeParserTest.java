package org.heigit.bigspatialdata.oshdb.util.time;

import static org.heigit.bigspatialdata.oshdb.util.time.IsoDateTimeParser.parseIsoDateTime;
import static org.heigit.bigspatialdata.oshdb.util.time.IsoDateTimeParser.parseIsoPeriod;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Period;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class IsoDateTimeParserTest {

  @Test
  public void testParseIsoDateTime() {
    // test allowed variants

    //Basic Dates
    String[] yyyy = {"2020-01-01T00:00Z", "2020"};
    String[] yyyymm = {"2020-02-01T00:00Z", "202002"};
    String[] yyyymmdd = {"2020-02-17T00:00Z", "20200217"};

    Assert.assertEquals(yyyy[0], parseIsoDateTime(yyyy[1]).toString());
    Assert.assertEquals(yyyymm[0], parseIsoDateTime(yyyymm[1]).toString());
    Assert.assertEquals(yyyymmdd[0], parseIsoDateTime(yyyymmdd[1]).toString());

    //Extended Dates
    String[] yyyyMm = {"2020-02-01T00:00Z", "2020-02"};
    String[] yyyyMmDd = {"2020-02-17T00:00Z", "2020-02-17"};

    Assert.assertEquals(yyyyMm[0], parseIsoDateTime(yyyyMm[1]).toString());
    Assert.assertEquals(yyyyMmDd[0], parseIsoDateTime(yyyyMmDd[1]).toString());

    //Extended Date-Time
    String[] yyyyMmDdHh = {"2020-02-17T23:00Z", "2020-02-17T23"};
    String[] yyyyMmDdHhz = {"2020-02-17T23:00Z", "2020-02-17T23Z"};

    String[] yyyyMmDdHhMm = {"2020-02-17T23:55Z", "2020-02-17T23:55"};
    String[] yyyyMmDdHhMmz = {"2020-02-17T23:55Z", "2020-02-17T23:55Z"};

    String[] yyyyMmDdHhMmSs = {"2020-02-17T23:55:12Z", "2020-02-17T23:55:12"};
    String[] yyyyMmDdHhMmSsz = {"2020-02-17T23:55:12Z", "2020-02-17T23:55:12Z"};

    String[] yyyyMmDdHhMmSsSss = {"2020-02-17T23:55:12.999Z", "2020-02-17T23:55:12.999"};
    String[] yyyyMmDdHhMmSsSssz = {"2020-02-17T23:55:12.999Z", "2020-02-17T23:55:12.999Z"};

    Assert.assertEquals(yyyyMmDdHh[0], parseIsoDateTime(yyyyMmDdHh[1]).toString());
    Assert.assertEquals(yyyyMmDdHhz[0], parseIsoDateTime(yyyyMmDdHhz[1]).toString());

    Assert.assertEquals(yyyyMmDdHhMm[0], parseIsoDateTime(yyyyMmDdHhMm[1]).toString());
    Assert.assertEquals(yyyyMmDdHhMmz[0], parseIsoDateTime(yyyyMmDdHhMmz[1]).toString());

    Assert.assertEquals(yyyyMmDdHhMmSs[0], parseIsoDateTime(yyyyMmDdHhMmSs[1]).toString());
    Assert.assertEquals(yyyyMmDdHhMmSsz[0], parseIsoDateTime(yyyyMmDdHhMmSsz[1]).toString());

    Assert.assertEquals(yyyyMmDdHhMmSsSss[0], parseIsoDateTime(yyyyMmDdHhMmSsSss[1]).toString());
    Assert.assertEquals(yyyyMmDdHhMmSsSssz[0], parseIsoDateTime(yyyyMmDdHhMmSsSssz[1]).toString());

  }


  @Test(expected = OSHDBTimestampException.class)
  public void throwsNegativeDateParseIsoDateTime() {
    //Negative Dates
    String nyyyy = "-0333";
    parseIsoDateTime(nyyyy);
  }

  @Test(expected = OSHDBTimestampException.class)
  public void throwsShortYearParseIsoDateTime() {
    //Short Year
    String yy = "12";
    parseIsoDateTime(yy);
  }

  @Test(expected = OSHDBTimestampException.class)
  public void throwsPosTimezoneHhParseIsoDateTime() {
    String posTimezoneHh = "2020-02-17T23:55+02";
    parseIsoDateTime(posTimezoneHh);
  }

  @Test(expected = OSHDBTimestampException.class)
  public void throwsPosTimezoneHhMmParseIsoDateTime() {
    String posTimezoneHhmm = "2020-02-17T23:55+0230";
    parseIsoDateTime(posTimezoneHhmm);
  }

  @Test(expected = OSHDBTimestampException.class)
  public void throwsPosTimezoneHh_MmParseIsoDateTime() {
    String posTimezoneHhMm = "2020-02-17T23:55+02:30";
    parseIsoDateTime(posTimezoneHhMm);
  }

  //
  @Test(expected = OSHDBTimestampException.class)
  public void throwsNegTimezoneHhParseIsoDateTime() {
    String negTimezoneHh = "2020-02-17T23:55-02";
    parseIsoDateTime(negTimezoneHh);
  }

  @Test(expected = OSHDBTimestampException.class)
  public void throwsNegTimezoneHhMmParseIsoDateTime() {
    String negTimezoneHhMm = "2020-02-17T23:55-0230";
    parseIsoDateTime(negTimezoneHhMm);
  }

  @Test(expected = OSHDBTimestampException.class)
  public void throwsNegTimezoneHh_MmParseIsoDateTime() {
    String negTimezoneHhMm = "2020-02-17T23:55-02:30";
    parseIsoDateTime(negTimezoneHhMm);
  }

  @Test(expected = DateTimeException.class)
  public void throwsWrongDateParseIsoDateTime() {
    //Wrong Date
    String wrongDateTime = "2020-13-01T00:00";
    parseIsoDateTime(wrongDateTime);
  }


  @Test
  public void testParseIsoPeriod() {
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
    Map<String, Object> fullYearMonthDayTime = parseIsoPeriod(fullDateTimePeriod[0]);
    Period fullYearMonthDayTimePeriod = (Period) fullYearMonthDayTime.get("period");
    Duration fullYearMonthDayTimeDuration = (Duration) fullYearMonthDayTime.get("duration");
    Assert.assertEquals(fullDateTimePeriod[1], fullYearMonthDayTimePeriod.toString());
    Assert.assertEquals(fullDateTimePeriod[2], fullYearMonthDayTimeDuration.toString());

    // Period output should be same as input, Duration should be ZERO
    String fullDatePeriod = "P1Y3M10D";
    Map<String, Object> fullYearMonthDay = parseIsoPeriod(fullDatePeriod);
    Period fullYearMonthDayPeriod = (Period) fullYearMonthDay.get("period");
    Duration fullYearMonthDayDuration = (Duration) fullYearMonthDay.get("duration");
    Assert.assertEquals(fullDatePeriod, fullYearMonthDayPeriod.toString());
    Assert.assertTrue(fullYearMonthDayDuration.isZero());

    // Period output should be same as input, Duration should be ZERO
    String shortDatePeriod = "P3M10D";
    Map<String, Object> shortMonthDay = parseIsoPeriod(shortDatePeriod);
    Period shortMonthDayPeriod = (Period) shortMonthDay.get("period");
    Duration shortMonthDayDuration = (Duration) shortMonthDay.get("duration");
    Assert.assertEquals(shortDatePeriod, shortMonthDayPeriod.toString());
    Assert.assertTrue(shortMonthDayDuration.isZero());

    // Period should equal 14 days, Duration should be ZERO
    String weekPeriod = "P2W";
    Map<String, Object> twoWeeks = parseIsoPeriod(weekPeriod);
    Period twoWeeksPeriod = (Period) twoWeeks.get("period");
    Duration twoWeeksDuration = (Duration) twoWeeks.get("duration");
    Assert.assertEquals(14, twoWeeksPeriod.getDays());
    Assert.assertTrue(twoWeeksDuration.isZero());

  }

  @Test(expected = OSHDBTimestampException.class)
  public void throwsFormatParseIsoPeriod() {
    // test throw exeption for unsupported formats
    parseIsoPeriod("PT1Y2M");
  }

  @Test(expected = OSHDBTimestampException.class)
  public void throwsZeroLengthParseIsoPeriod() {
    //test for zero length ISOPeriod
    parseIsoPeriod("PT0S");
  }

}
