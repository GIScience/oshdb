package org.heigit.ohsome.oshdb.osm;

import java.util.function.Predicate;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;

/**
 * Interface for single version osm-element relation.
 */
public interface OSMRelation extends OSMEntity {

  @Override
  default OSMType getType() {
    return OSMType.RELATION;
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
   * @param memberFilter apply filter to Stream of members
   * @return stream of member entities (OSM)
   */
  Stream<OSMEntity> getMemberEntities(OSHDBTimestamp timestamp, Predicate<OSMMember> memberFilter);

  /**
   * Returns a stream of all member entities (OSM) for the given timestamp.
   *
   * @param timestamp the timestamp for the osm member entity
   * @return stream of member entities (OSM)
   */
  Stream<OSMEntity> getMemberEntities(OSHDBTimestamp timestamp);

}
