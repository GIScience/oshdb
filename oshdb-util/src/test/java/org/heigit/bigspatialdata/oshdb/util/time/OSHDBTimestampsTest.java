package org.heigit.bigspatialdata.oshdb.util.time;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps.Interval;
import org.junit.Test;

public class OSHDBTimestampsTest {

  @Test
  public void testTimeIntervals() {
    List<List<Object>> inputs = new ArrayList<>();
    inputs.add(Arrays.asList("2008-01-31T12:34:56", "2010-01-31T12:34:56", Interval.YEARLY,
        Arrays.asList("2008-01-31T12:34:56", "2009-01-31T12:34:56", "2010-01-31T12:34:56")
    ));
    inputs.add(Arrays.asList("2008-01-31T12:34:56", "2008-07-31T12:34:56", Interval.QUARTERLY,
        Arrays.asList( "2008-01-31T12:34:56", "2008-04-30T12:34:56", "2008-07-30T12:34:56")
    ));
    inputs.add(Arrays.asList("2008-01-31T12:34:56", "2008-03-31T12:34:56", Interval.MONTHLY,
        Arrays.asList( "2008-01-31T12:34:56", "2008-02-29T12:34:56", "2008-03-29T12:34:56")
    ));
    inputs.add(Arrays.asList("2008-01-31T12:34:56", "2008-02-14T12:34:56", Interval.WEEKLY,
        Arrays.asList( "2008-01-31T12:34:56", "2008-02-07T12:34:56", "2008-02-14T12:34:56")
    ));
    inputs.add(Arrays.asList("2008-01-31T12:34:56", "2008-02-02T12:34:56", Interval.DAILY,
        Arrays.asList( "2008-01-31T12:34:56", "2008-02-01T12:34:56", "2008-02-02T12:34:56")
    ));
    inputs.add(Arrays.asList("2008-01-31T12:34:56", "2008-01-31T15:00:00", Interval.HOURLY,
        Arrays.asList( "2008-01-31T12:34:56", "2008-01-31T13:34:56", "2008-01-31T14:34:56")
    ));

    List<Interval> testedIntervals = new ArrayList<>();
    for (List<Object> testobj : inputs) {
      OSHDBTimestamps timestamps = new OSHDBTimestamps((String) testobj.get(0),
          (String) testobj.get(1),
          (Interval) testobj.get(2));
      SortedSet<OSHDBTimestamp> result = timestamps.get();
      Iterable<String> expResult = (Iterable<String>) testobj.get(3);

      Iterator resultIt = result.iterator();
      Iterator expResultIt = expResult.iterator();
      while (resultIt.hasNext()) {
        assertEquals(expResultIt.next(), resultIt.next().toString());
      }
      assertFalse(expResultIt.hasNext());
      testedIntervals.add((Interval) testobj.get(2));
    }

    for (Interval interval : EnumSet.allOf(Interval.class)) {
      assertTrue(testedIntervals.contains(interval));
    }
  }

}
