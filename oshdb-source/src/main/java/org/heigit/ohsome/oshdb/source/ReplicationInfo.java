package org.heigit.ohsome.oshdb.source;


import java.time.ZonedDateTime;

public interface ReplicationInfo {

  static ReplicationInfo of(String url, String timestamp, int sequenceNumber) {
    return new ReplicationInfo() {

      @Override
      public ZonedDateTime getTimestamp() {
        return ZonedDateTime.parse(timestamp);
      }

      @Override
      public int getSequenceNumber() {
        return sequenceNumber;
      }

      @Override
      public String getBaseUrl() {
        return url;
      }
    };
  }

  String getBaseUrl();

  ZonedDateTime getTimestamp();

  int getSequenceNumber();

}
