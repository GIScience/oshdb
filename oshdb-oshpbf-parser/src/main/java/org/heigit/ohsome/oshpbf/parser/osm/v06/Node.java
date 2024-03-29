package org.heigit.ohsome.oshpbf.parser.osm.v06;

import org.heigit.ohsome.oshdb.osm.OSMType;

public class Node extends Entity {

  public final long longitude;
  public final long latitude;

  public Node(CommonEntityData entityData, long longitude, long latitude) {
    super(entityData);
    this.longitude = longitude;
    this.latitude = latitude;
  }

  @Override
  public OSMType getType() {
    return OSMType.NODE;
  }

  public long getLongitude() {
    return longitude;
  }

  public long getLatitude() {
    return latitude;
  }

  @Override
  public String toString() {
    return String.format("%s (lon:%d,lat%d)", super.toString(), longitude, latitude);
  }

}
