package org.heigit.ohsome.oshdb.osm;

import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;

/**
 * Interface for single version osm-element way.
 */
public interface OSMWay extends OSMEntity {

  @Override
  default OSMType getType() {
    return OSMType.WAY;
  }

  /**
   * Returns the members for this current version.
   *
   * @return OSMMember for this version
   */
  OSMMember[] getMembers();

  /**
   * Returns a stream of all member entities (OSM) for the given timestamp.
   *
   * @param timestamp the timestamp for the osm member entity
   * @return stream of member entities (OSM)
   */
  Stream<OSMNode> getMemberEntities(OSHDBTimestamp timestamp);
}
