package org.heigit.bigspatialdata.oshdb.util.geometry.helpers;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.util.taginterpreter.TagInterpreter;

public abstract class FakeTagInterpreter implements TagInterpreter {
  @Override
  public boolean isArea(OSMEntity entity) {
    return false;
  }

  @Override
  public boolean isLine(OSMEntity entity) {
    return false;
  }

  @Override
  public boolean hasInterestingTagKey(OSMEntity osm) {
    return false;
  }

  @Override
  public boolean isMultipolygonOuterMember(OSMMember osmMember) {
    return false;
  }

  @Override
  public boolean isMultipolygonInnerMember(OSMMember osmMember) {
    return false;
  }

  @Override
  public boolean isOldStyleMultipolygon(OSMRelation osmRelation) {
    return false;
  }
}
