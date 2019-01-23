package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import java.io.Serializable;

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
}
