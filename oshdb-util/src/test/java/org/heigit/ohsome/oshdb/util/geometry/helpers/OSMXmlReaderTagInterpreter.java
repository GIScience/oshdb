package org.heigit.ohsome.oshdb.util.geometry.helpers;

import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;

/**
 * An implementation of the tag interpreter for use with {@link OSMXmlReader}.
 */
public class OSMXmlReaderTagInterpreter extends FakeTagInterpreter {
  private final int area;
  private final int areaYes;
  private final int type;
  private final int typeMultipolygon;
  private final int emptyRole;
  private final int outer;
  private final int inner;

  /**
   * Constructor reading all required values from a given {@link OSMXmlReader}.
   */
  public OSMXmlReaderTagInterpreter(OSMXmlReader osmXmlReader) {
    area = osmXmlReader.keys().getOrDefault("area", -1);
    areaYes = area == -1 ? -1 : osmXmlReader.keyValues().get(area).getOrDefault("yes", -1);
    type = osmXmlReader.keys().getOrDefault("type", -1);
    typeMultipolygon = type == -1 ? -1 : osmXmlReader.keyValues().get(type)
        .getOrDefault("multipolygon", -1);
    emptyRole = osmXmlReader.roles().getOrDefault("", -1);
    outer = osmXmlReader.roles().getOrDefault("outer", -1);
    inner = osmXmlReader.roles().getOrDefault("inner", -1);
  }

  @Override
  public boolean isArea(OSMEntity e) {
    if (e instanceof OSMWay) {
      OSMMember[] nds = ((OSMWay) e).getMembers();
      return nds.length >= 4 && nds[0].getId() == nds[nds.length - 1].getId()
          && e.getTags().hasTag(area, areaYes);
    }
    if (e instanceof OSMRelation) {
      return e.getTags().hasTag(type, typeMultipolygon);
    }
    return true;
  }

  @Override
  public boolean isMultipolygonOuterMember(OSMMember osmMember) {
    return osmMember.getType() == OSMType.WAY
        && (osmMember.getRole().getId() == outer || osmMember.getRole().getId() == emptyRole);
  }

  @Override
  public boolean isMultipolygonInnerMember(OSMMember osmMember) {
    return osmMember.getType() == OSMType.WAY && osmMember.getRole().getId() == inner;
  }
}
