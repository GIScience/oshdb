package org.heigit.bigspatialdata.oshdb.osh2;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

/**
 * The root interface in the <i>osh hierarchy</i>.
 */
public interface OSH<OSM extends OSMEntity> extends Iterable<OSM> {

  
  
  /**
   * Return the id of the osh object
   * @return the id of osh object
   */
  long getId();
    
  /**
   * Returns the type of this osh object.
   * @return type
   */
  OSMType type();
  
  @Override
  Iterator<OSM> iterator();

  default Stream<OSM> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

}
