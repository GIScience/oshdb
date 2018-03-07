package org.heigit.bigspatialdata.oshdb.util.time;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ISODateTimeParser {

  /**
   * Converts an ISO 8601 Date or combined Date-Time String into a UTC based ZonedDateTime Object.
   * 
   *  Examples:
   *  ISO Date: 2017
   *  ISO Date: 2016-03
   *  ISO Date: 2015-06-05
   *  Basic ISO Date: 20170305
   *  combined: 2011-10-03T20[Z]
   *  combined: 2011-10-03T20:15[Z]
   *  combined: 2011-10-03T20:15:25[Z]
   *  
   * @param isoDateTimeString ISO Date or ISO DateTime string
   * @return ZonedDateTime
   * @throws Exception
   */
  public static ZonedDateTime parseISODateTime(String isoDateTimeString) throws Exception{
    ZonedDateTime zdt = null;
    int length = isoDateTimeString.length();
    
    switch (length) {
    case 4:
      // pattern = "uuuu";
      zdt = Year.parse(isoDateTimeString).atDay(1).atStartOfDay(ZoneId.of("Z"));
      break;
    case 6:
      // pattern = "uuuuMM";
      zdt = LocalDate.parse(isoDateTimeString+"01", DateTimeFormatter.BASIC_ISO_DATE).atStartOfDay(ZoneId.of("Z"));
      break;
    case 7:
      // pattern = "uuuu-MM";
      zdt = YearMonth.parse(isoDateTimeString).atDay(1).atStartOfDay(ZoneId.of("Z"));
      break;
    case 8:
      // pattern = "uuuuMMDD";
      zdt = LocalDate.parse(isoDateTimeString, DateTimeFormatter.BASIC_ISO_DATE).atStartOfDay(ZoneId.of("Z"));;
      break;
    case 10:
      // pattern = "uuuu-MM-DD";
      zdt = LocalDate.parse(isoDateTimeString, DateTimeFormatter.ISO_DATE).atStartOfDay(ZoneId.of("Z"));
      break;
      //with time
    case 13:
      // pattern = "uuuu-MM-DDTHH";
      zdt = LocalDateTime.parse(isoDateTimeString+":00:00Z", DateTimeFormatter.ISO_DATE_TIME).atZone(ZoneId.of("Z"));
      break;
    case 16:
      // pattern = "uuuu-MM-DDTHH:MM";
      zdt = LocalDateTime.parse(isoDateTimeString+":00Z", DateTimeFormatter.ISO_DATE_TIME).atZone(ZoneId.of("Z"));
      break;
    case 19:
      // pattern = "uuuu-MM-DDTHH:MM:ss";
      zdt = LocalDateTime.parse(isoDateTimeString+"Z", DateTimeFormatter.ISO_DATE_TIME).atZone(ZoneId.of("Z"));
      break;
    case 20:
      // pattern = "uuuu-MM-DDTHH:MM:ssZ";
      zdt = ZonedDateTime.parse(isoDateTimeString, DateTimeFormatter.ISO_DATE_TIME);
      break;
    default:
      throw new Exception("Date or DateTime Format not supported: " + isoDateTimeString);
    }
    
    return zdt;
  }

    /**
     * Converts an ISO Period string into two parts, a period for the date part and a duration for the time part.
     * 
     * Examples:
     *  ISO Date Period: P1Y (years)
     *  ISO Date Period: P3M (months)
     *  ISO Date Period: P1D (days)
     *  mixed ISO Dates:  P1Y25D or 1M15D
     *  ISO Date Period: P2W (weeks)
     *  ISO Time Duration: PT1H (hours)
     *  ISO Time Duration: PT30M (minutes)
     *  ISO Time Duration: PT20S (seconds)
     *  combined DateTime: P1Y3M5DT5H30M20S
     *  
     * @param isoPeriodString ISO Period string
     * @return HashMap with a Period object and a Duration object
     * @throws Exception
     */
    public static Map<String, Object> parseISOPeriod(String isoPeriodString) throws Exception{
      
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
      //    Full DateTime Period: PnYnMnDTnHnMnS, e.g P1Y3M10DT1H15M25S (1 year 3months 10 days 1 hour 15 minutes and 25 seconds)
      //    Full Date Period:      PnYnMnD, e.g. P1Y3M10D (1 year 3 months and 10 days)
      //    Short Date Period:    e.g PnY or PnMnD or any combination of years, months and days
      //    Week Period:           PnW, e.g. P2W 2 weeks
      //    Full Time Duration:   PTnHnMnS, e.g. PT1H3M25S (1 hour 3 minutes and 25 seconds)
      //    Short Time Duration:  PTnH or any combonation of hours, minutes and seconds (PT1H10S 1 hour and 10 seconds)
      
      // validate period
      Pattern isValidPattern = Pattern.compile("^P(?!$)(\\d+Y)?(\\d+M)?(\\d+W)?(\\d+D)?(T(?=\\d+[HMS])(\\d+H)?(\\d+M)?(\\d+S)?)?$", Pattern.MULTILINE );
      Matcher matcher  = isValidPattern.matcher(isoPeriodString);
      boolean isValid = matcher.matches();
      
      if (!isValid) {
        throw new Exception("isoPeriodString is not valid: " + isoPeriodString + " (Format: P[yY][mM][dD][T[hH][mM][sS]])");
      }
      
      boolean hasDateAndTimeComponent = Pattern.matches("P.+T.+", isoPeriodString);
      boolean hasOnlyTimeComponent = isoPeriodString.startsWith("PT");
      
      if (hasDateAndTimeComponent){
        String[] periodParts = isoPeriodString.split("T");
        period = Period.parse(periodParts[0]);
        duration = Duration.parse("PT"+periodParts[1]);
      } else if (hasOnlyTimeComponent) {
        duration = Duration.parse(isoPeriodString);
      } else { //hasOnlyDateComponent
        period = Period.parse(isoPeriodString);
      }
      
      // check against zero length period/duration
      if (duration.equals(Duration.ZERO) && period.equals(Period.ZERO)){
        throw new Exception("the specified period has a lenght of ZERO: " + isoPeriodString);
      }
      
      Map<String, Object> result = new HashMap<>();
      result.put("period", period);
      result.put("duration", duration);
      
      return result;
    }
}
