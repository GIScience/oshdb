package org.heigit.ohsome.oshdb.util.geometry.helpers;

import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMWay;

public class FakeTagInterpreterAreaAlways extends FakeTagInterpreter {
  @Override
  public boolean isArea(OSMEntity e) {
    if (e instanceof OSMWay) {
      OSMMember[] nds = ((OSMWay) e).getRefs();
      return (nds.length >= 4 && nds[0].getId() == nds[nds.length - 1].getId());
    }
    return true;
  }
}
