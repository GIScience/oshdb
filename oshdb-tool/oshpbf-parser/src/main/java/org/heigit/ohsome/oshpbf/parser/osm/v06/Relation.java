package org.heigit.ohsome.oshpbf.parser.osm.v06;

import org.heigit.ohsome.oshdb.osm.OSMType;

public class Relation extends Entity {

  public final RelationMember[] members;

  public Relation(CommonEntityData entityData, RelationMember[] members) {
    super(entityData);
    this.members = members;
  }

  @Override
  public OSMType getType() {
    return OSMType.RELATION;
  }

  public RelationMember[] getMembers() {
    return members;
  }

  @Override
  public String toString() {
    return "R " + super.toString() + " #members: " + members.length;
  }

}
