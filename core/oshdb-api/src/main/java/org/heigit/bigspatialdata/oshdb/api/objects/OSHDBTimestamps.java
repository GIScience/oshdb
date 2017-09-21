package org.heigit.bigspatialdata.oshdb.api.objects;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.heigit.bigspatialdata.oshdb.api.utils.ISODateTimeParser;

public class OSHDBTimestamps {
  
  private int startYear;
  private int endYear;
  private int startMonth;
  private int endMonth;
  private int startDay;
  private int endDay;


  public OSHDBTimestamps(int startYear, int endYear, int startMonth, int endMonth, int startDay, int endDay) {
    super();
    this.startYear = startYear;
    this.endYear = endYear;
    this.startMonth = startMonth;
    this.endMonth = endMonth;
    this.startDay = startDay;
    this.endDay = endDay;
  }

  public OSHDBTimestamps(int startYear, int endYear, int startMonth, int endMonth) {
    this(startYear, endYear, startMonth, endMonth, 1, 1);
  }

  public OSHDBTimestamps(int startYear, int endYear) {
    this(startYear, endYear, 1,1, 1, 1);
  }

  public List<Long> getTimestamps() {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    List<Long> timestamps = new ArrayList<>();
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    for (int year = startYear; year <= endYear; year++) {
      for (int month = startMonth; month <= endMonth; month++) {
        for (int day = startDay; day <= endDay; day++) {
          try {
            timestamps.add(formatter.parse(String.format("%d%02d%02d", year, month, day)).getTime() / 1000);
          } catch (Exception e) {
            System.err.println("error while parsing date in OSHDBTimestamps!");
            e.printStackTrace();
          }
        }
      }
    }
    return timestamps;
  }

  public List<OSHDBTimestamp> getOSHDBTimestamps() {
    return this.getTimestamps().stream().map(OSHDBTimestamp::new).collect(Collectors.toList());
  }

  
  public static  List<Long> getTimestampsAsEpochSeconds(String isoStringStart, String isoStringEnd, String isoStringPeriod) throws Exception{
    
    ZonedDateTime start = ISODateTimeParser.parseISODateTime(isoStringStart);
    ZonedDateTime end   = ISODateTimeParser.parseISODateTime(isoStringEnd);
    Map<String, Object> steps = ISODateTimeParser.parseISOPeriod(isoStringPeriod);
   
    Period period = (Period) steps.get("period");
    Duration duration = (Duration) steps.get("duration");
       
    //validate start and end. start should be before end.
    if (start.isAfter(end)){
      throw new Exception("Start is after end, but must be earlier: \n" + 
                          "start: " + start + 
                          "\nend: " + end);
    }
    
    List<Long> timestamps = new ArrayList<>();
    
    ZonedDateTime currentTimestamp = ZonedDateTime.from(start);
    long endTimestamp = end.toEpochSecond();
    
  
    while ( currentTimestamp.toEpochSecond() <= endTimestamp){
      timestamps.add(currentTimestamp.toEpochSecond());
      currentTimestamp = currentTimestamp.plus(period).plus(duration);
    }
    
  
//    System.out.println(period + " " + duration);
//    System.out.println(start +" - " + end);
    
    System.out.println("Generated " + timestamps.size() + " timestamps (in EpochSeconds since 1970) from " + start + " to " + Instant.ofEpochSecond(timestamps.get(timestamps.size() -1)));
    
  return timestamps; 
    
  }
  
  public static  List<Long> getTimestampsAsEpochSeconds(String isoStringStart, String isoStringPeriod, int numberOfTimestamps) throws Exception{
    
    //validate TODO throw exception?
    if (numberOfTimestamps < 1) return null;
    
    List<Long> timestamps = new ArrayList<>();
    
    ZonedDateTime start = ISODateTimeParser.parseISODateTime(isoStringStart);
    Map<String, Object> steps = ISODateTimeParser.parseISOPeriod(isoStringPeriod);
   
    Period period = (Period) steps.get("period");
    Duration duration = (Duration) steps.get("duration");
    
    int counter = 0;
    ZonedDateTime currentTimestamp = ZonedDateTime.from(start);
    
    while (counter < numberOfTimestamps){
      
      timestamps.add(currentTimestamp.toEpochSecond());
      currentTimestamp = currentTimestamp.plus(period).plus(duration);
      counter++;
    }
    
    System.out.println("Generated " + timestamps.size() + " timestamps (in EpochSeconds since 1970) from " + start + " to " + Instant.ofEpochSecond(timestamps.get(timestamps.size() -1)));
    return timestamps;
  }
  
}
