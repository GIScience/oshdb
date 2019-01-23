package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import java.io.Serializable;
import org.heigit.bigspatialdata.oshdb.api.generic.WeightedValue;

/**
 * Mutable version of WeightedValue type (for internal use to do faster aggregation).
 *
 * @param <X> an arbitrary (often numeric) payload data type
 */
class PayloadWithWeight<X> implements Serializable {
  X num;
  double weight;

  PayloadWithWeight(X num, double weight) {
    this.num = num;
    this.weight = weight;
  }

  static PayloadWithWeight<Double> identity() {
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
