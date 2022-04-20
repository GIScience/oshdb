package org.heigit.ohsome.oshdb.util.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps.Interval;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link OSHDBTimestamps} class.
 */
public class OSHDBTimestampsTest {

  @Test
  public void testTimeIntervals() {
    final List<String> startList = new ArrayList<>();
    final List<String> endList = new ArrayList<>();
    final List<Interval> intervalList = new ArrayList<>();
    final List<List<String>> expectedResultList = new ArrayList<>();

    startList.add("2008-01-31T12:34:56");
    endList.add("2010-01-31T12:34:56");
    intervalList.add(Interval.YEARLY);
    expectedResultList.add(
        Arrays.asList("2008-01-31T12:34:56", "2009-01-31T12:34:56", "2010-01-31T12:34:56"));

    startList.add("2008-01-31T12:34:56");
    endList.add("2008-07-31T12:34:56");
    intervalList.add(Interval.QUARTERLY);
    expectedResultList.add(
        Arrays.asList("2008-01-31T12:34:56", "2008-04-30T12:34:56", "2008-07-31T12:34:56"));

    startList.add("2008-01-31T12:34:56");
    endList.add("2008-03-31T12:34:56");
    intervalList.add(Interval.MONTHLY);
    expectedResultList.add(
        Arrays.asList("2008-01-31T12:34:56", "2008-02-29T12:34:56", "2008-03-31T12:34:56"));

    startList.add("2008-01-31T12:34:56");
    endList.add("2008-02-14T12:34:56");
    intervalList.add(Interval.WEEKLY);
    expectedResultList.add(
        Arrays.asList("2008-01-31T12:34:56", "2008-02-07T12:34:56", "2008-02-14T12:34:56"));

    startList.add("2008-01-31T12:34:56");
    endList.add("2008-02-02T12:34:56");
    intervalList.add(Interval.DAILY);
    expectedResultList.add(
        Arrays.asList("2008-01-31T12:34:56", "2008-02-01T12:34:56", "2008-02-02T12:34:56"));

    startList.add("2008-01-31T12:34:56");
    endList.add("2008-01-31T15:00:00");
    intervalList.add(Interval.HOURLY);
    expectedResultList.add(
        Arrays.asList("2008-01-31T12:34:56", "2008-01-31T13:34:56", "2008-01-31T14:34:56"));

    // check if lists have the same size
    assertEquals(startList.size(), endList.size());
    assertEquals(startList.size(), intervalList.size());
    assertEquals(startList.size(), expectedResultList.size());

    Set<Interval> testedIntervals = EnumSet.noneOf(Interval.class);
    for (int i = 0; i < startList.size(); i++) {
      OSHDBTimestamps timestamps = new OSHDBTimestamps(startList.get(i), endList.get(i),
          intervalList.get(i));

      var resultIt = timestamps.get().iterator();
      var expResultIt = expectedResultList.get(i).iterator();
      // check if results are exactly the same
      while (resultIt.hasNext()) {
        assertEquals(expResultIt.next(), resultIt.next().toString());
      }
      // check if more results are expected
      assertFalse(expResultIt.hasNext());
      testedIntervals.add(intervalList.get(i));
    }

    // check if all intervals in the enum are tested
    assertTrue(testedIntervals.containsAll(EnumSet.allOf(Interval.class)));
  }

  @Test()
  public void testInvalidTimestamp() {
    assertThrows(RuntimeException.class, () -> {
      new OSHDBTimestamps("test123");
    });
  }

}
