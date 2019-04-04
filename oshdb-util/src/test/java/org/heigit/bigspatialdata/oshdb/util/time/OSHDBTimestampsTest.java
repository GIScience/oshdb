package org.heigit.bigspatialdata.oshdb.util.time;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps.Interval;
import org.junit.Test;

public class OSHDBTimestampsTest {

  @Test
  public void testTimeIntervals() {
    List<String> startList = new ArrayList<>();
    List<String> endList = new ArrayList<>();
    List<Interval> intervalList = new ArrayList<>();
    List<List<String>> expectedResultList = new ArrayList<>();

    startList.add("2008-01-31T12:34:56");
    endList.add("2010-01-31T12:34:56");
    intervalList.add(Interval.YEARLY);
    expectedResultList.add(Arrays.asList("2008-01-31T12:34:56", "2009-01-31T12:34:56", "2010-01-31T12:34:56"));

    startList.add("2008-01-31T12:34:56");
    endList.add("2008-07-31T12:34:56");
    intervalList.add(Interval.QUARTERLY);
    expectedResultList.add(Arrays.asList("2008-01-31T12:34:56", "2008-04-30T12:34:56", "2008-07-30T12:34:56"));

    startList.add("2008-01-31T12:34:56");
    endList.add("2008-03-31T12:34:56");
    intervalList.add(Interval.MONTHLY);
    expectedResultList.add(Arrays.asList("2008-01-31T12:34:56", "2008-02-29T12:34:56", "2008-03-29T12:34:56"));

    startList.add("2008-01-31T12:34:56");
    endList.add("2008-02-14T12:34:56");
    intervalList.add(Interval.WEEKLY);
    expectedResultList.add(Arrays.asList("2008-01-31T12:34:56", "2008-02-07T12:34:56", "2008-02-14T12:34:56"));

    startList.add("2008-01-31T12:34:56");
    endList.add("2008-02-02T12:34:56");
    intervalList.add(Interval.DAILY);
    expectedResultList.add(Arrays.asList("2008-01-31T12:34:56", "2008-02-01T12:34:56", "2008-02-02T12:34:56"));

    startList.add("2008-01-31T12:34:56");
    endList.add("2008-01-31T15:00:00");
    intervalList.add(Interval.HOURLY);
    expectedResultList.add(Arrays.asList("2008-01-31T12:34:56", "2008-01-31T13:34:56", "2008-01-31T14:34:56"));

    // check if lists have the same size
    assertEquals(startList.size(), endList.size());
    assertEquals(startList.size(), intervalList.size());
    assertEquals(startList.size(), expectedResultList.size());

    List<Interval> testedIntervals = new ArrayList<>();
    for (int i = 0; i < startList.size(); i++) {
      OSHDBTimestamps timestamps = new OSHDBTimestamps(startList.get(i), endList.get(i),
          intervalList.get(i));

      Iterator resultIt = timestamps.get().iterator();
      Iterator expResultIt = expectedResultList.get(i).iterator();
      // check if results are exactly the same
      while (resultIt.hasNext()) {
        assertEquals(expResultIt.next().toString(), resultIt.next().toString());
      }
      // check if more results are expected
      assertFalse(expResultIt.hasNext());
      testedIntervals.add(intervalList.get(i));
    }

    // check if all intervals in the enum are tested
    for (Interval interval : EnumSet.allOf(Interval.class)) {
      assertTrue(testedIntervals.contains(interval));
    }
  }

}
