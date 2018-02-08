package org.heigit.bigspatialdata.oshdb.osh2;

import java.util.Iterator;
import java.util.stream.Stream;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm2.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;

public interface OSHEntity {

  public long getId();
  public OSHDBBoundingBox getBoundingBox();
  
  public abstract OSMType getType();
  
  
  public Iterable<? extends OSMEntity> versions();
  
  public Iterator<? extends OSMEntity> iterator();
  
  public Stream<? extends OSMEntity> stream();
  
  public OSHEntity copy();
}
