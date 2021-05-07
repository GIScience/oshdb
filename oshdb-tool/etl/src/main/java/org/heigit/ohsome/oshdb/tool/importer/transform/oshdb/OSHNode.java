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
  OSHNode copy();

  @Override
  Iterator<OSMNode> iterator();

  @Override
  Iterable<OSMNode> versions();

  @Override
  default Stream<OSMNode> stream() {
    return StreamSupport.stream(versions().spliterator(), false);
  }
}
