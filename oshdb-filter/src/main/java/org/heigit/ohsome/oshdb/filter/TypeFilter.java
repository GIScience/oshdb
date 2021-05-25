package org.heigit.ohsome.oshdb.filter;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.jetbrains.annotations.Contract;

/**
 * A filter which selects OSM entities by their OSM type (i.e., node, way or relation).
 */
public class TypeFilter implements FilterExpression {
  private final OSMType type;

  TypeFilter(OSMType type) {
    this.type = type;
  }

  @Contract(pure = true)
  public OSMType getType() {
    return this.type;
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return entity.getType() == type;
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return entity.getType() == type;
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
    return "type:" + type.toString().toLowerCase();
  }
}
