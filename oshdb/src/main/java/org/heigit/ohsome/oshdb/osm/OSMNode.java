package org.heigit.ohsome.oshdb.osm;

/**
 * Interface for single version osm-element node.
 *
 */
public interface OSMNode extends OSMEntity {

  @Override
  default OSMType getType() {
    return OSMType.NODE;
  }

  public double getLongitude();

  public double getLatitude();

  public int getLon();

  public int getLat();
}
