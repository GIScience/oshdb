package org.heigit.ohsome.oshdb.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import java.util.List;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.heigit.ohsome.oshdb.util.LCS.Sequence;

class LCSTest {

  private int[] transform(Sequence actions, int[] a, int[] b) {
    if (actions.isEmpty()) {
      return b;
    }
    int[] result = new int[b.length];
    int apos = 0;
    int bpos = 0;
    int pos = 0;
    for (var action : actions) {
      int size = action.getValue().intValue();
      switch (action.getKey()) {
        case ADD:
          System.arraycopy(b, bpos, result, pos, size);
          bpos += size;
          pos += size;
          break;
        case SKIP:
          apos += size;
          break;
        case TAKE:
          System.arraycopy(a, apos, result, pos, size);
          apos += size;
          bpos += size;
          pos += size;
          break;
        case UPDATE:
          System.arraycopy(b, bpos, result, pos, size);
          bpos += size;
          apos += size;
          pos += size;
          break;
        default:
          break;
      }
    }
    return result;
  }

  @Property
  void test(
      @ForAll List<@IntRange(min = 0, max = 100) Integer> a,
      @ForAll List<@IntRange(min = 0, max = 100) Integer> b) {
    int[] from = a.stream().mapToInt(Integer::intValue).toArray();
    int[] expected = b.stream().mapToInt(Integer::intValue).toArray();

    var sequence = LCS.lcs(from, expected);
    int[] actual = transform(sequence, from, expected);
    assertArrayEquals(expected, actual);
  }
}
