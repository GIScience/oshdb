package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import com.google.common.collect.Streams;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

/**
 * Represents a filter expression which can be applied on OSM/OSH entities.
 *
 * <p>Such an expression might be a simple key=value tag filter, or a more complex combination
 * of boolean operators, parentheses, tag filters and/or other filters.</p>
 */
interface FilterExpression {
  /**
   * Apply the filter to an OSM entity.
   *
   * @param e the OSM entity to check.
   * @return true if the entity fulfills the specified filter, otherwise false.
   */
  boolean applyOSM(OSMEntity e);

  /**
   * Apply the filter to an OSH entity.
   *
   * <p>Must return the same as <code>oshEntity.getVersions().….anyMatch(applyOSM)</code>.</p>
   *
   * @param e the OSH entity to check.
   * @return true if the at least one of the OSH entity's versions fulfills the specified filter,
   *         false otherwise.
   */
  default boolean applyOSH(OSHEntity e) {
    return Streams.stream(e.getVersions()).anyMatch(this::applyOSM);
  }
}
