package org.heigit.ohsome.oshdb.util.geometry.helpers;

import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;

/**
 * A dummy implementation of the {@link TagInterpreter} interface which interprets all
 * multipolygon members as outer ways.
 */
public class FakeTagInterpreterAreaMultipolygonAllOuters extends FakeTagInterpreterAreaAlways {
  @Override
  public boolean isMultipolygonOuterMember(OSMMember osmMember) {
    return true;
  }
}
