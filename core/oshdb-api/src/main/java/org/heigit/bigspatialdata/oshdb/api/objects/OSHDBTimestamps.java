package org.heigit.bigspatialdata.oshdb.api.objects;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class OSHDBTimestamps {
  
  private int startYear;
  private int endYear;
  private int startMonth;
  private int endMonth;
  private int startDay;
  private int endDay;

  private final SimpleDateFormat formatter;


  public OSHDBTimestamps(int startYear, int endYear, int startMonth, int endMonth, int startDay, int endDay) {
    super();
    this.startYear = startYear;
    this.endYear = endYear;
    this.startMonth = startMonth;
    this.endMonth = endMonth;
    this.startDay = startDay;
    this.endDay = endDay;
    this.formatter = new SimpleDateFormat("yyyyMMdd");
  }

  public OSHDBTimestamps(int startYear, int endYear, int startMonth, int endMonth) {
    this(startYear, endYear, startMonth, endMonth, 1, 1);
  }

  public OSHDBTimestamps(int startYear, int endYear) {
    this(startYear, endYear, 1,1, 1, 1);
  }



  public List<Long> getTimestamps() {
    List<Long> timestamps = new ArrayList<>();
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    for (int year = startYear; year <= endYear; year++) {
      for (int month = startMonth; month <= endMonth; month++) {
        for (int day = startDay; day <= endDay; day++) {
          try {
            timestamps.add(formatter.parse(String.format("%d%02d%02d", year, month, day)).getTime() / 1000);
          } catch (java.text.ParseException e) {
            System.err.println("error, please check your code!");
          }
        }
      }
    }
    return timestamps;
  }

  public List<OSHDBTimestamp> getOSHDBTimestamps() {
    return this.getTimestamps().stream().map(OSHDBTimestamp::new).collect(Collectors.toList());
  }

}
