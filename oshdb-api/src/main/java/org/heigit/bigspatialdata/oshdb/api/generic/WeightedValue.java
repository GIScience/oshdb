package org.heigit.bigspatialdata.oshdb.api.generic;

/**
 * Immutable object that stores a numeric value and an associated weight.
 * Used to specify data input for the calculation of weighted averages.
 * @param <X> A numeric data type for the value.
 */
public class WeightedValue<X extends Number> {
  private X value;
  private double weight;

  public WeightedValue(X value, double weight) {
    this.value = value;
    this.weight = weight;
  }

  /**
   * @return the stored numeric value
   */
  public X getValue() {
    return value;
  }

  /**
   * @return the value's associated weight
   */
  public double getWeight() {
    return weight;
  }
}
