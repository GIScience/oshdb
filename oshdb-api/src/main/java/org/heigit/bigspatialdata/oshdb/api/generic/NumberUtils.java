package org.heigit.bigspatialdata.oshdb.api.generic;

import java.io.Serializable;
import org.jetbrains.annotations.NotNull;

public class NumberUtils implements Serializable {
  /**
   * Add two numeric values and return the sum in the smallest common type.
   *
   * <p>Supports Integer, Float and Double values</p>
   *
   * @param x a numeric value
   * @param y a numeric value
   * @param <T> a numeric data type
   * @return the sum of x and y
   */
  @NotNull
  public static <T extends Number> T add(T x, T y) {
    if (x instanceof Integer && y instanceof Integer) {
      return (T) (Integer) (x.intValue() + y.intValue());
    } else if (x instanceof Float && (y instanceof Float || y instanceof Integer)) {
      return (T) (Float) (x.floatValue() + y.floatValue());
    } else {
      return (T) (Double) (x.doubleValue() + y.doubleValue());
    }
  }
}
