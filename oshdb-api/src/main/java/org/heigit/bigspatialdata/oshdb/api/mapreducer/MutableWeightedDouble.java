package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import java.io.Serializable;
import org.heigit.bigspatialdata.oshdb.api.generic.WeightedValue;

/**
 * Mutable version of WeightedValue type.
 *
 * <p>For internal use to do faster aggregation during reduce operations.</p>
 */
class MutableWeightedDouble implements Serializable {
  double num;
  double weight;

  private MutableWeightedDouble(double num, double weight) {
    this.num = num;
    this.weight = weight;
  }

  static MutableWeightedDouble identitySupplier() {
    return new MutableWeightedDouble(0.0, 0.0);
  }

  static MutableWeightedDouble accumulator(
      MutableWeightedDouble acc,
      WeightedValue cur) {
    acc.num = acc.num + cur.getValue().doubleValue() * cur.getWeight();
    acc.weight += cur.getWeight();
    return acc;
  }

  static MutableWeightedDouble combiner(
      MutableWeightedDouble a,
      MutableWeightedDouble b) {
    return new MutableWeightedDouble(a.num + b.num, a.weight + b.weight);
  }
}
