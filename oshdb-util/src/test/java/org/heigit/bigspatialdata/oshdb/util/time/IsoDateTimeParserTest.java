package org.heigit.bigspatialdata.oshdb.util.time;

import java.time.DateTimeException;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.Period;
import java.util.Map;

import static org.heigit.bigspatialdata.oshdb.util.time.IsoDateTimeParser.parseIsoDateTime;
import static org.heigit.bigspatialdata.oshdb.util.time.IsoDateTimeParser.parseIsoPeriod;

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
        String[] yyyy_mm = {"2020-02-01T00:00Z", "2020-02"};
        String[] yyyy_mm_dd = {"2020-02-17T00:00Z", "2020-02-17"};

        Assert.assertEquals(yyyy_mm[0], parseIsoDateTime(yyyy_mm[1]).toString());
        Assert.assertEquals(yyyy_mm_dd[0], parseIsoDateTime(yyyy_mm_dd[1]).toString());

        //Extended Date-Time
        String[] yyyy_mm_dd_hh = {"2020-02-17T23:00Z", "2020-02-17T23"};
        String[] yyyy_mm_dd_hhz = {"2020-02-17T23:00Z", "2020-02-17T23Z"};

        String[] yyyy_mm_dd_hh_mm = {"2020-02-17T23:55Z", "2020-02-17T23:55"};
        String[] yyyy_mm_dd_hh_mmz = {"2020-02-17T23:55Z", "2020-02-17T23:55Z"};

        String[] yyyy_mm_dd_hh_mm_ss = {"2020-02-17T23:55:12Z", "2020-02-17T23:55:12"};
        String[] yyyy_mm_dd_hh_mm_ssz = {"2020-02-17T23:55:12Z", "2020-02-17T23:55:12Z"};

        String[] yyyy_mm_dd_hh_mm_ss_sss = {"2020-02-17T23:55:12.999Z", "2020-02-17T23:55:12.999"};
        String[] yyyy_mm_dd_hh_mm_ss_sssz = {"2020-02-17T23:55:12.999Z", "2020-02-17T23:55:12.999Z"};

        Assert.assertEquals(yyyy_mm_dd_hh[0], parseIsoDateTime(yyyy_mm_dd_hh[1]).toString());
        Assert.assertEquals(yyyy_mm_dd_hhz[0], parseIsoDateTime(yyyy_mm_dd_hhz[1]).toString());

        Assert.assertEquals(yyyy_mm_dd_hh_mm[0], parseIsoDateTime(yyyy_mm_dd_hh_mm[1]).toString());
        Assert.assertEquals(yyyy_mm_dd_hh_mmz[0], parseIsoDateTime(yyyy_mm_dd_hh_mmz[1]).toString());

        Assert.assertEquals(yyyy_mm_dd_hh_mm_ss[0], parseIsoDateTime(yyyy_mm_dd_hh_mm_ss[1]).toString());
        Assert.assertEquals(yyyy_mm_dd_hh_mm_ssz[0], parseIsoDateTime(yyyy_mm_dd_hh_mm_ssz[1]).toString());

        Assert.assertEquals(yyyy_mm_dd_hh_mm_ss_sss[0], parseIsoDateTime(yyyy_mm_dd_hh_mm_ss_sss[1]).toString());
        Assert.assertEquals(yyyy_mm_dd_hh_mm_ss_sssz[0], parseIsoDateTime(yyyy_mm_dd_hh_mm_ss_sssz[1]).toString());

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
    public void throwsPosTimezoneHHParseIsoDateTime() {
        String posTimezone_hh = "2020-02-17T23:55+02";
        parseIsoDateTime(posTimezone_hh);
    }

    @Test(expected = OSHDBTimestampException.class)
    public void throwsPosTimezoneHHMMParseIsoDateTime() {
        String posTimezone_hhmm = "2020-02-17T23:55+0230";
        parseIsoDateTime(posTimezone_hhmm);
    }

    @Test(expected = OSHDBTimestampException.class)
    public void throwsPosTimezoneHH_MMParseIsoDateTime() {
        String posTimezone_hh_mm = "2020-02-17T23:55+02:30";
        parseIsoDateTime(posTimezone_hh_mm);
    }

    //
    @Test(expected = OSHDBTimestampException.class)
    public void throwsNegTimezoneHHParseIsoDateTime() {
        String negTimezone_hh = "2020-02-17T23:55-02";
        parseIsoDateTime(negTimezone_hh);
    }

    @Test(expected = OSHDBTimestampException.class)
    public void throwsNegTimezoneHHMMParseIsoDateTime() {
        String negTimezone_hhmm = "2020-02-17T23:55-0230";
        parseIsoDateTime(negTimezone_hhmm);
    }

    @Test(expected = OSHDBTimestampException.class)
    public void throwsNegTimezoneHH_MMParseIsoDateTime() {
        String negTimezone_hh_mm = "2020-02-17T23:55-02:30";
        parseIsoDateTime(negTimezone_hh_mm);
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

        //    Full DateTime Period: PnYnMnDTnHnMnS, e.g P1Y3M10DT1H15M25S (1 year 3months 10 days 1 hour 15 minutes and 25 seconds)
        //    Full Date Period:      PnYnMnD, e.g. P1Y3M10D (1 year 3 months and 10 days)
        //    Short Date Period:    e.g PnY or PnMnD or any combination of years, months and days
        //    Week Period:           PnW, e.g. P2W 2 weeks
        //    Full Time Duration:   PTnHnMnS, e.g. PT1H3M25S (1 hour 3 minutes and 25 seconds)
        //    Short Time Duration:  PTnH or any combination of hours, minutes and seconds (PT1H10S 1 hour and 10 seconds)

        String[] fullDateTimePeriod = {"P1Y3M10DT1H15M25S", "P1Y3M10D", "PT1H15M25S"}; // input, output Period, output Duration
        Map<String, Object> fullYearMonthDayTime = parseIsoPeriod(fullDateTimePeriod[0]);
        Period fullYearMonthDayTimePeriod = (Period) fullYearMonthDayTime.get("period");
        Duration fullYearMonthDayTimeDuration = (Duration) fullYearMonthDayTime.get("duration");
        Assert.assertEquals(fullDateTimePeriod[1], fullYearMonthDayTimePeriod.toString());
        Assert.assertEquals(fullDateTimePeriod[2], fullYearMonthDayTimeDuration.toString());

        String fullDatePeriod = "P1Y3M10D";  //Period output should be same as input, Duration should be ZERO;
        Map<String, Object> fullYearMonthDay = parseIsoPeriod(fullDatePeriod);
        Period fullYearMonthDayPeriod = (Period) fullYearMonthDay.get("period");
        Duration fullYearMonthDayDuration = (Duration) fullYearMonthDay.get("duration");
        Assert.assertEquals(fullDatePeriod, fullYearMonthDayPeriod.toString());
        Assert.assertTrue(fullYearMonthDayDuration.isZero());

        String shortDatePeriod = "P3M10D"; //Period output should be same as input, Duration should be ZERO;
        Map<String, Object> shortMonthDay = parseIsoPeriod(shortDatePeriod);
        Period shortMonthDayPeriod = (Period) shortMonthDay.get("period");
        Duration shortMonthDayDuration = (Duration) shortMonthDay.get("duration");
        Assert.assertEquals(shortDatePeriod, shortMonthDayPeriod.toString());
        Assert.assertTrue(shortMonthDayDuration.isZero());

        String weekPeriod = "P2W"; // Period should equal 14 days, Duration should be ZERO;
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
