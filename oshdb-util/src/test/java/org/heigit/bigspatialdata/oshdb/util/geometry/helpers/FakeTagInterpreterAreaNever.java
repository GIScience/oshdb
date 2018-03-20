package org.heigit.bigspatialdata.oshdb.util.geometry.helpers;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

public class FakeTagInterpreterAreaNever extends FakeTagInterpreter {
  @Override
  public boolean isArea(OSMEntity e) {
    return false;
  }
}
