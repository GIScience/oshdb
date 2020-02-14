package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

/**
 * A boolean "not" of a sub-expression.
 */
public class NotOperator extends UnaryOperator {
  NotOperator(FilterExpression sub) {
    super(sub);
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    return !sub.applyOSM(e);
  }

  @Override
  public String toString() {
    return "not(" + sub.toString() + ")";
  }
}
