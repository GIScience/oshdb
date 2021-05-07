package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import java.util.Iterator;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.osm.OSMType;

public interface OSHEntity {

  long getId();

  OSHDBBoundingBox getBoundingBox();

  OSMType getType();

  Iterable<? extends OSMEntity> versions();

  Iterator<? extends OSMEntity> iterator();

  Stream<? extends OSMEntity> stream();

  OSHEntity copy();
}
