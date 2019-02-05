package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import java.io.Serializable;
import org.heigit.bigspatialdata.oshdb.api.generic.WeightedValue;

/**
 * Mutable version of WeightedValue type.
 *
 * <p>For internal use to do faster aggregation during reduce operations.</p>
 *
 * @param <X> an arbitrary (often numeric) payload data type
 */
class PayloadWithWeight<X> implements Serializable {
  X num;
  double weight;

  private PayloadWithWeight(X num, double weight) {
    this.num = num;
    this.weight = weight;
  }

  static PayloadWithWeight<Double> identitySupplier() {
    return new PayloadWithWeight<>(0.0, 0.0);
  }

  static PayloadWithWeight<Double> accumulator(
      PayloadWithWeight<Double> acc,
      WeightedValue cur) {
    acc.num = acc.num + cur.getValue().doubleValue() * cur.getWeight();
    acc.weight += cur.getWeight();
    return acc;
  }

  static PayloadWithWeight<Double> combiner(
      PayloadWithWeight<Double> a,
      PayloadWithWeight<Double> b) {
    return new PayloadWithWeight<>(a.num + b.num, a.weight + b.weight);
  }
}
