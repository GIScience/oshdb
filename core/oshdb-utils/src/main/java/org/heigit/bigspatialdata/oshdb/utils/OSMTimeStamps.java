package org.heigit.bigspatialdata.oshdb.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

public class OSMTimeStamps {
  
  private int startYear;
  private int endYear;
  private int startMonth;
  private int endMonth;
  
  
  
  public OSMTimeStamps(int startYear, int endYear, int startMonth, int endMonth) {
    super();
    this.startYear = startYear;
    this.endYear = endYear;
    this.startMonth = startMonth;
    this.endMonth = endMonth;
  }



  public List createTimeStamps(){
    List<Long> timestamps = new ArrayList<>();
    final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    for (int year = startYear; year <= endYear; year++) {
      for (int month = startMonth; month <= endMonth; month++) {
        try {
          timestamps.add(formatter.parse(String.format("%d%02d01", year, month)).getTime() / 1000);
        } catch (java.text.ParseException e) {
          System.err.println("basdoawrd");
        };
      }
    }
   Collections.sort(timestamps, Collections.reverseOrder());
   return timestamps;
  }

}
