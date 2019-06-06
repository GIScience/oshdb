package org.heigit.bigspatialdata.oshdb.api.util.contributionevaluator.object;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;

/**
 * A modified tag consisting of a state before and after the modification.
 */
public class OSHDBModifiedTag {

  private final OSHDBTag after;
  private final OSHDBTag before;

  /**
   * A modified Tag.
   *
   * @param before Status before modification.
   * @param after Status after modification.
   */
  public OSHDBModifiedTag(OSHDBTag before, OSHDBTag after) {
    this.before = before;
    this.after = after;
  }

  /**
   * Get the new Tag.
   *
   * @return the after
   */
  public OSHDBTag getAfter() {
    return after;
  }

  /**
   * Get the old Tag.
   *
   * @return the before
   */
  public OSHDBTag getBefore() {
    return before;
  }

}
