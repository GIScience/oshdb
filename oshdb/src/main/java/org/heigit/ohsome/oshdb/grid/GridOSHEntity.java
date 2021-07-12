package org.heigit.ohsome.oshdb.grid;

import java.io.Serializable;
import java.util.Locale;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.index.XYGrid;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.CellId;

public abstract class GridOSHEntity
    implements Serializable {

  private static final long serialVersionUID = 1L;
  protected final long id;
  protected final int level;

  protected final long baseTimestamp;

  protected final int baseLongitude;
  protected final int baseLatitude;

  protected final long baseId;

  protected final int[] index;
  protected final byte[] data;

  /**
   * Base constructor {@code GridOSHEntity}.
   */
  protected GridOSHEntity(final long id, final int level, final long baseId,
      final long baseTimestamp, final int baseLongitude, final int baseLatitude, final int[] index,
      final byte[] data) {

    this.id = id;
    this.level = level;
    this.baseTimestamp = baseTimestamp;
    this.baseLongitude = baseLongitude;
    this.baseLatitude = baseLatitude;
    this.baseId = baseId;

    this.index = index;
    this.data = data;
  }

  public long getId() {
    return id;
  }

  public int getLevel() {
    return level;
  }

  public abstract Iterable<? extends OSHEntity> getEntities();

  @Override
  public String toString() {
    if (id >= 0) {
      OSHDBBoundingBox bbox = XYGrid.getBoundingBox(new CellId((int) id, level));
      return String.format(Locale.ENGLISH, "ID:%d Level:%d %s", id, level, bbox);
    } else {
      return String.format(Locale.ENGLISH, "ID:%d Level:%d", id, level);
    }
  }
}
