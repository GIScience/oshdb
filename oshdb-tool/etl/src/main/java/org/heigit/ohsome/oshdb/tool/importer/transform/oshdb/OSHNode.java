package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.heigit.ohsome.oshdb.osm.OSMType;

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
