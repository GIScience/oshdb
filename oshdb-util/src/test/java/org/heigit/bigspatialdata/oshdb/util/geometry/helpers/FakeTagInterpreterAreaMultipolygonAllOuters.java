package org.heigit.bigspatialdata.oshdb.util.geometry.helpers;

import org.heigit.bigspatialdata.oshdb.osm.OSMMember;

public class FakeTagInterpreterAreaMultipolygonAllOuters extends FakeTagInterpreterAreaAlways {
  @Override
  public boolean isMultipolygonOuterMember(OSMMember osmMember) {
    return true;
  }
}
