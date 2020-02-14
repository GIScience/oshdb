package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

/**
 * A boolean "or" of two sub-expressions.
 */
public class OrOperator extends BinaryOperator {
  OrOperator(FilterExpression e1, FilterExpression e2) {
    super(e1, e2);
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    return e1.applyOSM(e) || e2.applyOSM(e);
  }

  @Override
  public String toString() {
    return e1.toString() + " or " + e2.toString();
  }
}
