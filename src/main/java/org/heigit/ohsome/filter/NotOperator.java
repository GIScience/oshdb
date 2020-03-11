package org.heigit.ohsome.filter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.locationtech.jts.geom.Geometry;

/**
 * A boolean "not" of a sub-expression.
 */
public class NotOperator extends UnaryOperator {
  NotOperator(FilterExpression sub) {
    super(sub);
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return !op.applyOSM(entity);
  }

  @Override
  public boolean applyOSMGeometry(OSMEntity entity, Geometry geometry) {
    return !op.applyOSMGeometry(entity, geometry);
  }

  @Override
  public FilterExpression negate() {
    return op;
  }

  @Override
  public String toString() {
    return "not(" + op.toString() + ")";
  }
}
