package org.heigit.ohsome.filter;

import java.io.Serializable;
import java.util.function.ToDoubleFunction;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.locationtech.jts.geom.Geometry;

/**
 * A filter which executes a check based on OSM feature geometries.
 */
public abstract class GeometryFilter extends NegatableFilter {
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

  interface GeometryMetricEvaluator extends ToDoubleFunction<Geometry>, Serializable {
    double applyAsDouble(Geometry geometry);

    static GeometryMetricEvaluator fromLambda(ToDoubleFunction<Geometry> func, String name) {
      return new GeometryMetricEvaluator() {
        @Override
        public double applyAsDouble(Geometry geometry) {
          return func.applyAsDouble(geometry);
        }

        @Override
        public String toString() {
          return name;
        }
      };
    }
  }

  interface RangedFilter extends FilterInternal {
    ValueRange getRange();
  }

  protected GeometryFilter(
      @Nonnull ValueRange valueRange,
      GeometryMetricEvaluator metricEvaluator
  ) {
    super(new RangedFilter() {
      @Override
      public boolean applyOSH(OSHEntity entity) {
        return true;
      }

      @Override
      public boolean applyOSM(OSMEntity entity) {
        return true;
      }

      @Override
      public boolean applyOSMGeometry(OSMEntity entity, Geometry geometry) {
        return valueRange.test(metricEvaluator.applyAsDouble(geometry));
      }

      @Override
      public String toString() {
        return metricEvaluator.toString() + ":" + valueRange.toString();
      }

      @Override
      public ValueRange getRange() {
        return valueRange;
      }
    });
  }

  public ValueRange getRange() {
    return ((RangedFilter) this.filter).getRange();
  }
}
