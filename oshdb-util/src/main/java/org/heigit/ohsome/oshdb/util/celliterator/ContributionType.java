package org.heigit.ohsome.oshdb.util.celliterator;

/**
 * Type of contribution to an OSM entity.
 */
public enum ContributionType {
  /** a new object has been created */
  CREATION,
  /** one object has been deleted */
  DELETION,
  /** at least one tag of this object has been modified */
  TAG_CHANGE,
  /** the geometry of the object has been altered */
  GEOMETRY_CHANGE
}
