package org.heigit.ohsome.oshpbf.parser.osm.v06;

public class RelationMember {

  public final long memId;
  public final String role;
  public final int type;

  public RelationMember(long memId, String role, int type) {
    this.memId = memId;
    this.role = role;
    this.type = type;
  }
}