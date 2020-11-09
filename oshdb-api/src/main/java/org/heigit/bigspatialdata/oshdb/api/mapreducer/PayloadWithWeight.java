package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import java.io.Serializable;
import org.heigit.bigspatialdata.oshdb.api.generic.WeightedValue;

/**
 * Mutable version of WeightedValue type.
 *
 * <p>For internal use to do faster aggregation during reduce operations.</p>
 */
class PayloadWithWeight implements Serializable {
  double num;
  double weight;

  private PayloadWithWeight(double num, double weight) {
    this.num = num;
    this.weight = weight;
  }

  static PayloadWithWeight identitySupplier() {
    return new PayloadWithWeight(0.0, 0.0);
  }

  static PayloadWithWeight accumulator(
      PayloadWithWeight acc,
      WeightedValue cur) {
    acc.num = acc.num + cur.getValue().doubleValue() * cur.getWeight();
    acc.weight += cur.getWeight();
    return acc;
  }

  static PayloadWithWeight combiner(
      PayloadWithWeight a,
      PayloadWithWeight b) {
    return new PayloadWithWeight(a.num + b.num, a.weight + b.weight);
  }
}
