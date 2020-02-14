package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

public class AndOperator extends BinaryOperator {
  AndOperator(FilterExpression e1, FilterExpression e2) {
    super(e1, e2);
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    return e1.applyOSM(e) && e2.applyOSM(e);
  }

  @Override
  public String toString() {
    return e1.toString() + " and " + e2.toString();
  }
}
