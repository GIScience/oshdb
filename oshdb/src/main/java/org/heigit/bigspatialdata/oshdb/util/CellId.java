package org.heigit.bigspatialdata.oshdb.util;

import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper class for cellIds consisting of a zoom level and an zoom-level specific id
 */
public class CellId implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(CellId.class);

  private final int zoomLevel;
  private final long id;

  /**
   * As every cellid is by definition linked to a zoomLevel, this class combines
   * this information.
   *
   * @param zoomLevel between 0:30
   * @param id >=0
   */
  public CellId(int zoomLevel, long id) {
    if (id < -1 || zoomLevel < 0 || zoomLevel > 30) {
      LOG.error("zoomLevel or id out of range");
      // todo: what about the "-1" cell for garbage data.
      // todo: also check for id<=2^zoomLevel
    }
    //reasonable to check, if ID fits to zoomLevel?
    this.zoomLevel = zoomLevel;
    this.id = id;
  }

  public static long getLevelId(int zoomlevel, long id) {
    return ((long) zoomlevel) << 56 | id;
  }

  public long getLevelId() {
    return getLevelId(zoomLevel, id);
  }

  public static CellId fromLevelId(long levelId) {
    final long id = levelId & 0x00FFFFFFFFFFFFFFL;
    final int zoomlevel = (int) (levelId >>> 56);
    return new CellId(zoomlevel, id);
  }

  /**
   *
   * @return
   */
  public long getId() {
    return this.id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (!(o instanceof CellId)) {
      return false;
    }
    CellId cellId = (CellId) o;
    return cellId.getId() == this.id && cellId.getZoomLevel() == this.zoomLevel;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 47 * hash + this.zoomLevel;
    hash = 47 * hash + (int) (this.id ^ (this.id >>> 32));
    return hash;
  }

  @Override
  public String toString() {
    return "CellId{" + "zoomLevel=" + zoomLevel + ", id=" + id + '}';
  }

  /**
   *
   * @return
   */
  public int getZoomLevel() {
    return this.zoomLevel;
  }

}
