package org.heigit.bigspatialdata.oshdb.util.geometry.helpers;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;

public class OSMXmlReaderTagInterpreter extends FakeTagInterpreter {

  private int area;
  private int areaYes;
  private int type;
  private int typeMultipolygon;
  private int outer;
  private int inner;

  public OSMXmlReaderTagInterpreter(OSMXmlReader osmXmlReader) {
    area = osmXmlReader.keys().get("area");
    areaYes = osmXmlReader.keyValues().get(area).get("yes");
    type = osmXmlReader.keys().get("type");
    typeMultipolygon = osmXmlReader.keyValues().get(type).get("multipolygon");
    outer = osmXmlReader.roles().get("outer");
    inner = osmXmlReader.roles().get("inner");
  }

  @Override
  public boolean isArea(OSMEntity e) {
    if (e instanceof OSMWay) {
      OSMMember[] nds = ((OSMWay) e).getRefs();
      return nds.length >= 4 && nds[0].getId() == nds[nds.length - 1].getId() &&
          e.hasTagValue(area, areaYes);
    }
    if (e instanceof OSMRelation) {
      return e.hasTagValue(type, typeMultipolygon);
    }
    return true;
  }

  @Override
  public boolean isMultipolygonOuterMember(OSMMember osmMember) {
    return osmMember.getRawRoleId() == outer;
  }

  @Override
  public boolean isMultipolygonInnerMember(OSMMember osmMember) {
    return osmMember.getRawRoleId() == inner;
  }
}
