package org.heigit.ohsome.oshdb.filter;

import java.io.Serializable;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.geometry.Geo;
import org.locationtech.jts.geom.Geometry;

/**
 * A filter which executes a check based on OSM feature geometries.
 */
public class GeometryFilter implements FilterExpression {

  /**
   * A filter which checks the length of OSM feature geometries.
   */
  static FilterExpression length(ValueRange range) {
    return new GeometryFilter(range, "length", Geo::lengthOf, false);
  }

  /**
   * A filter which checks the area of OSM feature geometries.
   */
  static FilterExpression area(ValueRange range) {
    return new GeometryFilter(range, "area", Geo::areaOf, false);
  }

  static class ValueRange implements Serializable {
    private final double fromValue;
    private final double toValue;

    ValueRange(double fromValue, double toValue) {
      this.fromValue = fromValue;
      this.toValue = toValue;
    }

    private boolean test(double value) {
      return value >= fromValue && value <= toValue;
    }

    @Override
    public String toString() {
      return (fromValue == Double.NEGATIVE_INFINITY ? "" : fromValue)
          + ".."
          + (toValue == Double.POSITIVE_INFINITY ? "" : toValue);
    }

    public double getFromValue() {
      return fromValue;
    }

    public double getToValue() {
      return toValue;
    }
  }

  interface GeometryMetricEvaluator extends ToDoubleFunction<Geometry>, Serializable {}

  private final ValueRange valueRange;
  private final String metric;
  private final GeometryMetricEvaluator metricEvaluator;
  private final boolean negated;

  private GeometryFilter(
      @Nonnull ValueRange valueRange,
      String metric,
      GeometryMetricEvaluator metricEvaluator,
      boolean negated
  ) {
    this.valueRange = valueRange;
    this.metric = metric;
    this.metricEvaluator = metricEvaluator;
    this.negated = negated;
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return true;
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return true;
  }

  @Override
  public boolean applyOSMGeometry(OSMEntity entity, Supplier<Geometry> geometrySupplier) {
    return valueRange.test(metricEvaluator.applyAsDouble(geometrySupplier.get())) ^ negated;
  }

  @Override
  public String toString() {
    return (negated ? "not " : "") + metric + ":" + valueRange.toString();
  }

  public ValueRange getRange() {
    return valueRange;
  }
}
