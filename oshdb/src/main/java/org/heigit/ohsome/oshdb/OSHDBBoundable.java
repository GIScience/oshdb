package org.heigit.ohsome.oshdb;

public interface OSHDBBoundable {
  
  long getMinLonLong();
  
  long getMinLatLong();
  
  long getMaxLonLong();
  
  long getMaxLatLong();
  
  default OSHDBBoundingBox getBoundingBox() {
    return new OSHDBBoundingBox(getMinLonLong(), getMinLatLong(), getMaxLonLong(), getMaxLatLong());
  }
  
  default double getMinLon() {
    return getMinLonLong() * OSHDB.GEOM_PRECISION;
  }
  
  default double getMinLat() {
    return getMinLatLong() * OSHDB.GEOM_PRECISION;
  }

  default double getMaxLon() {
    return getMaxLonLong() * OSHDB.GEOM_PRECISION;
  }

  default double getMaxLat() {
    return getMaxLatLong() * OSHDB.GEOM_PRECISION;
  }
  
  default boolean intersects(OSHDBBoundable otherBbox) {
    return (otherBbox != null)
        && (getMaxLatLong() >= otherBbox.getMinLatLong()) 
        && (getMinLatLong() <= otherBbox.getMaxLatLong()) 
        && (getMaxLonLong() >= otherBbox.getMinLonLong()) 
        && (getMinLonLong() <= otherBbox.getMaxLonLong());
  }
  
  default boolean isInside(OSHDBBoundingBox otherBbox) {
    return (otherBbox != null) 
        && (getMinLatLong() >= otherBbox.getMinLatLong()) 
        && (getMaxLatLong() <= otherBbox.getMaxLatLong())
        && (getMinLonLong() >= otherBbox.getMinLonLong()) 
        && (getMaxLonLong() <= otherBbox.getMaxLonLong());
  }
  
  default boolean isPoint() {
    return getMinLonLong() == getMaxLonLong() && getMinLatLong() == getMaxLatLong();
  }
}
