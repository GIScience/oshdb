package org.heigit.ohsome.oshdb.util.geometry.helpers;

import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;

/**
 * A dummy implementation of the {@link TagInterpreter} interface which interprets all OSM ways
 * as lines.
 */
public class FakeTagInterpreterAreaNever extends FakeTagInterpreter {
  @Override
  public boolean isArea(OSMEntity e) {
    return false;
  }
}
