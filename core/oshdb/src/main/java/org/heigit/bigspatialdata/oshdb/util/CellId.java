package org.heigit.bigspatialdata.oshdb.util;

import java.util.logging.Logger;

/**
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class CellId {

  private static final Logger LOG = Logger.getLogger(CellId.class.getName());
  private final int zoomlevel;
  private final long id;

  /**
   * As every cellid is by definition linked to a zoomlevel, this class combines
   * this information.
   *
   * @param zoomlevel between 0:30
   * @param id >=0
   * @throws org.heigit.bigspatialdata.oshdb.util.CellId.cellIdExeption
   */
  public CellId(int zoomlevel, long id) throws cellIdExeption {
    if (id < -1 || zoomlevel < 0 || zoomlevel > 30) {
      throw new cellIdExeption("zoomlevel or id out of range");
    }
    //reasonable to check, if ID fits to zoomlevel?
    this.zoomlevel = zoomlevel;
    this.id = id;
  }

  
  
  public long getLevelId(){
     return ((long)zoomlevel) << 56 | id;
  }
  
  public static CellId fromLevelId(long levelId) throws cellIdExeption{
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
    return cellId.getId() == this.id && cellId.getZoomLevel() == this.zoomlevel;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 47 * hash + this.zoomlevel;
    hash = 47 * hash + (int) (this.id ^ (this.id >>> 32));
    return hash;
  }

  @Override
  public String toString() {
    return "CellId{" + "zoomlevel=" + zoomlevel + ", id=" + id + '}';
  }

  /**
   *
   * @return
   */
  public int getZoomLevel() {
    return this.zoomlevel;
  }

  /**
   *
   */
  @SuppressWarnings("serial")
  public class cellIdExeption extends Exception {

    private cellIdExeption(String mymsg) {
      super(mymsg);
    }
  }

}
