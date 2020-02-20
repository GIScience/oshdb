package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public class TypeFilter implements Filter {
  private final OSMType type;

  TypeFilter(OSMType type) {
    this.type = type;
  }

  /**
   * Returns the OSM type of this filter.
   *
   * @return the OSM type of this filter.
   */
  public OSMType getType() {
    return this.type;
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    return e.getType() == type;
  }

  @Override
  public boolean applyOSH(OSHEntity e) {
    return e.getType() == type;
  }

  @Override
  public FilterExpression negate() {
    EnumSet<OSMType> otherTypes = EnumSet.of(OSMType.NODE, OSMType.WAY, OSMType.RELATION);
    otherTypes.remove(this.type);

    List<OSMType> otherTypesList = new ArrayList<>(otherTypes);
    assert otherTypesList.size() == 2 : "the negation of one osm type must equal exactly two types";

    return new OrOperator(
        new TypeFilter(otherTypesList.get(0)),
        new TypeFilter(otherTypesList.get(1))
    );
  }

  @Override
  public String toString() {
    return "type:" + type.toString();
  }
}
