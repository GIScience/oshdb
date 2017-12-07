package org.heigit.bigspatialdata.oshdb.api.generic;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

public class NumberUtils implements Serializable {
  @NotNull
  public static <T extends Number> T add(T x, T y) {
    if (x instanceof Integer && y instanceof Integer) return (T) (Integer) (x.intValue() + y.intValue());
    else if (x instanceof Float && (y instanceof Float || y instanceof Integer)) return (T) (Float) (x.floatValue() + y.floatValue());
    else return (T) (Double) (x.doubleValue() + y.doubleValue());
  }
}
