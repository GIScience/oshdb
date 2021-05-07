package org.heigit.ohsome.oshdb.util.time;

import static java.lang.Integer.signum;
import static org.junit.Assert.assertEquals;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.junit.Test;

/**
 * Test Suite for {@code OSHDBTimestampInterval}.
 */
public class OSHDBTimestampIntervalTest {

  /**
   * Test for the contract of {@code Comparable.compareTo}.
   */
  @Test
  public void testCompareTo() {
    var x = new OSHDBTimestampInterval(new OSHDBTimestamp(0), new OSHDBTimestamp(1));
    var y = new OSHDBTimestampInterval(new OSHDBTimestamp(0), new OSHDBTimestamp(2));

    assertEquals(-1, signum(x.compareTo(y)));
    assertEquals(1, signum(y.compareTo(x)));

    // The implementor must ensure sgn(x.compareTo(y)) == -sgn(y.compareTo(x)) for all x and y.
    // This implies that x.compareTo(y) must throw an exception iff y.compareTo(x)
    // throws an exception.
    assertEquals(true, signum(x.compareTo(y)) == signum(y.compareTo(x)) * -1);

    // The implementor must also ensure that the relation is transitive:
    // (x.compareTo(y) > 0 && y.compareTo(z) > 0) implies x.compareTo(z) > 0.
    var z = new OSHDBTimestampInterval(new OSHDBTimestamp(1), new OSHDBTimestamp(2));
    assertEquals(-1, signum(y.compareTo(z)));
    assertEquals(-1, signum(x.compareTo(z)));

    // Finally, the implementor must ensure that x.compareTo(y)==0 implies that
    // sgn(x.compareTo(z)) == sgn(y.compareTo(z)), for all z.
    y = new OSHDBTimestampInterval(new OSHDBTimestamp(0), new OSHDBTimestamp(1));
    assertEquals(0, x.compareTo(y));
    assertEquals(true, signum(x.compareTo(z)) == signum(y.compareTo(z)));
  }
}
