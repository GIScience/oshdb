package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import java.util.Iterator;
import java.util.stream.Stream;

import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.OSHDBBoundingBox;

public interface OSHEntity {

  public long getId();
  public OSHDBBoundingBox getBoundingBox();
  
  public abstract OSMType getType();
  
  
  public Iterable<? extends OSMEntity> versions();
  
  public Iterator<? extends OSMEntity> iterator();
  
  public Stream<? extends OSMEntity> stream();
  
  public OSHEntity copy();
}
