package org.heigit.ohsome.oshdb.grid;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.osh.OSHEntities;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHRelation;

public class GridOSHRelations extends GridOSHEntity implements Iterable<OSHRelation> {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new {@code GridOSHRelations} while rebase/compacting the input relations.
   *
   * @param id the grid id
   * @param level zoom level
   * @param baseId base of id for compact entities
   * @param baseTimestamp base of timemstamps for compact entities
   * @param baseLongitude base of longitude for compact entities
   * @param baseLatitude base of latitued for compact entities
   * @param list list of entities
   * @return new instance of this grid
   */
  public static GridOSHRelations compact(final long id, final int level, final long baseId,
          final long baseTimestamp, final int baseLongitude, final int baseLatitude,
          final List<OSHRelation> list) {

    int offset = 0;
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final int[] index = new int[list.size()];
    for (int i = 0; i < index.length; i++) {
      final OSHRelation osh = list.get(i);
      final ByteBuffer buffer =
          OSHRelationImpl.buildRecord(OSHEntities.toList(osh.getVersions()),
              osh.getNodes(), osh.getWays(), baseId, baseTimestamp, baseLongitude, baseLatitude);
      index[i] = offset;
      out.write(buffer.array(), 0, buffer.remaining());
      offset += buffer.remaining();
    }
    final byte[] data = out.toByteArray();
    return new GridOSHRelations(id, level, baseId, baseTimestamp, baseLongitude, baseLatitude,
        index, data);
  }

  private GridOSHRelations(final long id, final int level, final long baseId,
      final long baseTimestamp, final int baseLongitude, final int baseLatitude,
      final int[] index, final byte[] data) {
    super(id, level, baseId, baseTimestamp, baseLongitude, baseLatitude, index, data);
  }

  @Override
  public Iterable<? extends OSHEntity> getEntities() {
    return this;
  }

  @Override
  public Iterator<OSHRelation> iterator() {
    return new Iterator<>() {
      private int pos = 0;

      @Override
      public OSHRelation next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        int offset = index[pos];
        int length = (pos < index.length - 1 ? index[pos + 1] : data.length) - offset;
        pos++;
        return OSHRelationImpl.instance(data, offset, length, baseId, baseTimestamp,
            (int) baseLongitude, (int) baseLatitude);
      }

      @Override
      public boolean hasNext() {
        return pos < index.length;
      }
    };
  }

  @Override
  public String toString() {
    return String.format("Grid-Cell of OSHRelations %s", super.toString());
  }
}
