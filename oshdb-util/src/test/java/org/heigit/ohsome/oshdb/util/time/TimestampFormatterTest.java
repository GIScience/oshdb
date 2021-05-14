package org.heigit.ohsome.oshdb.util.time;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import org.junit.Test;

/**
 * Tests the {@link TimestampFormatter} class.
 */
public class TimestampFormatterTest {

  public TimestampFormatterTest() {
  }

  @Test
  public void testDate() {
    Date date = new Date(1510052557000L);
    TimestampFormatter instance = TimestampFormatter.getInstance();
    String expResult = "2017-11-07";
    String result = instance.date(date);
    assertEquals(expResult, result);
  }

  @Test
  public void testIsoDateTime_Date() {
    Date date = new Date(1510052557000L);
    TimestampFormatter instance = TimestampFormatter.getInstance();
    String expResult = "2017-11-07T11:02:37Z";
    String result = instance.isoDateTime(date);
    assertEquals(expResult, result);
  }

  @Test
  public void testIsoDateTime_long() {
    long timestamp = 1510052557L;
    TimestampFormatter instance = TimestampFormatter.getInstance();
    String expResult = "2017-11-07T11:02:37Z";
    String result = instance.isoDateTime(timestamp);
    assertEquals(expResult, result);
  }

}
