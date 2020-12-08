package org.heigit.bigspatialdata.oshdb.api.generic;

/**
 * Immutable object that stores a numeric value and an associated weight.
 * Used to specify data input for the calculation of weighted averages.
 */
public class WeightedValue {
  private final Number value;
  private final double weight;

  public WeightedValue(Number value, double weight) {
    this.value = value;
    this.weight = weight;
  }

  /**
   * Returns the stored numeric value.
   *
   * @return the stored numeric value
   */
  public Number getValue() {
    return value;
  }

  /**
   * Returns the stored weight.
   *
   * @return the value's associated weight
   */
  public double getWeight() {
    return weight;
  }
}
