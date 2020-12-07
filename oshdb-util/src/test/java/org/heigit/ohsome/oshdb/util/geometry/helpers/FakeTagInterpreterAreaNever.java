package org.heigit.ohsome.oshdb.util.geometry.helpers;

import org.heigit.ohsome.oshdb.osm.OSMEntity;

public class FakeTagInterpreterAreaNever extends FakeTagInterpreter {
  @Override
  public boolean isArea(OSMEntity e) {
    return false;
  }
}
