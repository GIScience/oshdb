package org.heigit.bigspatialdata.oshdb.util.celliterator;

public enum ContributionType {
  CREATION, // a new object has been created
  DELETION, // one object has been deleted
  TAG_CHANGE, // at least one tag of this object has been modified
  MEMBERLIST_CHANGE, // the member list of this object (way or relation) has changed in some way
  GEOMETRY_CHANGE // the geometry of the object has been modified either directly (see MEMBERLIST_CHANGE) or via changed coordinates of the object's child entities
}
