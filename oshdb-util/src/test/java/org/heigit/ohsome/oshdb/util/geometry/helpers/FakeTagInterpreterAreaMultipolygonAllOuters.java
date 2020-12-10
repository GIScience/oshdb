package org.heigit.ohsome.oshdb.util.geometry.helpers;

import org.heigit.ohsome.oshdb.osm.OSMMember;

public class FakeTagInterpreterAreaMultipolygonAllOuters extends FakeTagInterpreterAreaAlways {
  @Override
  public boolean isMultipolygonOuterMember(OSMMember osmMember) {
    return true;
  }
}
