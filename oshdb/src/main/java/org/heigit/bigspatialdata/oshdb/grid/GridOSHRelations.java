package org.heigit.bigspatialdata.oshdb.grid;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;

@SuppressWarnings("rawtypes")
public class GridOSHRelations extends GridOSHEntity {

  private static final long serialVersionUID = 1L;

  public static GridOSHRelations compact(final long id, final int level, final long baseId,
          final long baseTimestamp, final long baseLongitude, final long baseLatitude,
          final List<OSHRelation> list) throws IOException {

    int offset = 0;

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final int[] index = new int[list.size()];
    // TODO user iterator!!
    for (int i = 0; i < index.length; i++) {
      final OSHRelation c = list.get(i).rebase(baseId, baseTimestamp, baseLongitude, baseLatitude);
      final byte[] buffer = c.getData();
      index[i] = offset;
      out.write(buffer);
      offset += buffer.length;
    }
    final byte[] data = out.toByteArray();
    return new GridOSHRelations(id, level, baseId, baseTimestamp, baseLongitude, baseLatitude, index,
            data);
  }

  private GridOSHRelations(final long id, final int level, final long baseId, final long baseTimestamp,
          final long baseLongitude, final long baseLatitude, final int[] index, final byte[] data) {
    super(id, level, baseId, baseTimestamp, baseLongitude, baseLatitude, index, data);
  }

  @Override
  public Iterator<OSHRelation> iterator() {
    return new Iterator<OSHRelation>() {
      private int pos = 0;

      @Override
      public OSHRelation next() {
        int offset = index[pos];
        int length = ((pos < index.length - 1) ? index[pos + 1] : data.length) - offset;
        pos++;
        try {
          return OSHRelation.instance(data, offset, length, baseId, baseTimestamp, baseLongitude, baseLatitude);
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
