package org.heigit.bigspatialdata.oshdb.util.celliterator;

public enum ContributionType {
  CREATION, // a new object has been created
  DELETION, // one object has been deleted
  TAG_CHANGE, // at least one tag of this object has been modified
  GEOMETRY_CHANGE // the geometry of the object has been altered
}
