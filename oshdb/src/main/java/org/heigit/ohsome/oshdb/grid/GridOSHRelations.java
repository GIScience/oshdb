package org.heigit.ohsome.oshdb.grid;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.osh.OSHEntities;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHRelation;

public class GridOSHRelations extends GridOSHEntity implements Iterable<OSHRelation> {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new {@code GridOSHRelations} while rebase/compacting the input relations.
   */
  public static GridOSHRelations compact(final long id, final int level, final long baseId,
          final long baseTimestamp, final long baseLongitude, final long baseLatitude,
          final List<OSHRelation> list) throws IOException {

    int offset = 0;
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final int[] index = new int[list.size()];
    // TODO user iterator!!
    for (int i = 0; i < index.length; i++) {
      final OSHRelation osh = list.get(i);
      final ByteBuffer buffer = OSHRelationImpl.buildRecord(OSHEntities.toList(osh.getVersions()),
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
      final long baseTimestamp, final long baseLongitude, final long baseLatitude,
      final int[] index, final byte[] data) {
    super(id, level, baseId, baseTimestamp, baseLongitude, baseLatitude, index, data);
  }

  @Override
  public Iterable<? extends OSHEntity> getEntities() {
    return this;
  }

  @Override
  public Iterator<OSHRelation> iterator() {
    return new Iterator<OSHRelation>() {
      private int pos = 0;

      @Override
      public OSHRelation next() {
        int offset = index[pos];
        int length = (pos < index.length - 1 ? index[pos + 1] : data.length) - offset;
        pos++;
        try {
          return OSHRelationImpl.instance(data, offset, length, baseId, baseTimestamp,
              baseLongitude, baseLatitude);
        } catch (IOException e) {
          e.printStackTrace();
        }
        return null;
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
