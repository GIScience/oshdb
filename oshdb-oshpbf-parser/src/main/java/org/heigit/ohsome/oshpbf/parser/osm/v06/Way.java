package org.heigit.ohsome.oshpbf.parser.osm.v06;

import org.heigit.ohsome.oshdb.osm.OSMType;

public class Way extends Entity {

  public final long[] refs;

  public Way(CommonEntityData entityData, long[] refs) {
    super(entityData);
    this.refs = refs;
  }

  @Override
  public OSMType getType() {
    return OSMType.WAY;
  }

  public long[] getRefs() {
    return refs;
  }

}
