package org.heigit.bigspatialdata.oshdb.osh2;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm2.OSMNode;

public interface OSHNode extends OSHEntity {
  
  @Override
  default OSMType getType() {
    return OSMType.NODE;
  }

  @Override
  public OSHNode copy();
  
  @Override
  public Iterator<OSMNode> iterator();
  
  @Override
  public Iterable<OSMNode> versions();
  
  @Override
  public default Stream<OSMNode> stream(){
      return StreamSupport.stream(versions().spliterator(), false);
  }
}
