package org.heigit.ohsome.oshdb.osm;

import java.io.Serializable;
import org.heigit.ohsome.oshdb.OSHDBTags;
import org.heigit.ohsome.oshdb.OSHDBTemporal;

/**
 * Base interface for single version osm-elements.
 *
 */
public interface OSMEntity extends OSHDBTemporal, Serializable {


  OSMType getType();

  long getId();

  int getVersion();

  long getChangesetId();

  int getUserId();

  boolean isVisible();

  /**
   * Returns a "view" of the current osm tags.
   */
  OSHDBTags getTags();
}
