package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

/**
 * A boolean "and" of two sub-expressions.
 */
public class AndOperator extends BinaryOperator {
  AndOperator(FilterExpression e1, FilterExpression e2) {
    super(e1, e2);
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    return e1.applyOSM(e) && e2.applyOSM(e);
  }

  @Override
  public boolean applyOSH(OSHEntity e) {
    return e1.applyOSH(e) && e2.applyOSH(e);
  }

  @Override
  public String toString() {
    return e1.toString() + " and " + e2.toString();
  }
}
