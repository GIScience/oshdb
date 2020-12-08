package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import java.util.Arrays;

class TdigestReducer {

  /**
   * A COMPRESSION parameter of 1000 should provide relatively precise results, while not being
   * too demanding on memory usage. See page 20 in the paper [1]:
   *
   * <quote>
   *   &gt; Compression parameter (1/δ) was […] 1000 in order to reliably achieve 0.1% accuracy
   * </quote>
   *
   * <ul><li>
   *   [1] https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </li></ul>
   */
  private static final int COMPRESSION = 1000;

  static TDigest identitySupplier() {
    return new MergingDigest(COMPRESSION);
  }

  static <R extends Number> TDigest accumulator(TDigest acc, R cur) {
    acc.add(cur.doubleValue(), 1);
    return acc;
  }

  static TDigest combiner(TDigest a, TDigest b) {
    if (a.size() == 0) {
      return b;
    } else if (b.size() == 0) {
      return a;
    }
    MergingDigest r = new MergingDigest(COMPRESSION);
    r.add(Arrays.asList(a, b));
    return r;
  }
}
