package org.heigit.bigspatialdata.oshdb.api.util.contributionevaluator.object;

import java.util.List;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;

/**
 * A detailed colleciton of changed tags.
 */
public class OSHDBTagChange {

  private final List<OSHDBTag> added;
  private final List<OSHDBModifiedTag> changed;
  private final List<OSHDBTag> deleted;

  /**
   * Creates a collection of changed tags.
   *
   * @param added added tags.
   * @param deleted deleted tags.
   * @param changed modified tags.
   */
  public OSHDBTagChange(
      List<OSHDBTag> added,
      List<OSHDBTag> deleted,
      List<OSHDBModifiedTag> changed) {
    this.added = added;
    this.deleted = deleted;
    this.changed = changed;
  }

  /**
   * Get a list of added Keys-Values.
   *
   * @return the added
   */
  public List<OSHDBTag> getAdded() {
    return added;
  }

  /**
   * Get a list of deleted Key-Values.
   *
   * @return the deleted
   */
  public List<OSHDBTag> getDeleted() {
    return deleted;
  }

  /**
   * Get a list of Modified Tags.
   *
   * @return the changed
   */
  public List<OSHDBModifiedTag> getModified() {
    return changed;
  }

}
