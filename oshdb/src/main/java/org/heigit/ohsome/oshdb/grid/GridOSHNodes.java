package org.heigit.ohsome.oshdb.grid;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.osh.OSHEntities;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;

public class GridOSHNodes extends GridOSHEntity implements Iterable<OSHNode> {

  private static final long serialVersionUID = 1L;

  /**
   * Create a new {@code GridOSHNode} while rebasing the input nodes.
   */
  public static GridOSHNodes rebase(final long id, final int level, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude,
      final List<OSHNode> list) throws IOException {

    int offset = 0;
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final int[] index = new int[list.size()];
    for (int i = 0; i < index.length; i++) {
      final ByteBuffer buffer =
          OSHNodeImpl.buildRecord(OSHEntities.toList(list.get(i).getVersions()), baseId,
              baseTimestamp, baseLongitude, baseLatitude);
      index[i] = offset;
      out.write(buffer.array(), 0, buffer.remaining());
      offset += buffer.remaining();
    }
    final byte[] data = out.toByteArray();
    return new GridOSHNodes(id, level, baseId, baseTimestamp, baseLongitude, baseLatitude, index,
        data);
  }

  private GridOSHNodes(final long id, final int level, final long baseId, final long baseTimestamp,
      final long baseLongitude, final long baseLatitude, final int[] index, final byte[] data) {
    super(id, level, baseId, baseTimestamp, baseLongitude, baseLatitude, index, data);
  }

  @Override
  public Iterable<? extends OSHEntity> getEntities() {
    return this;
  }

  @Override
  public Iterator<OSHNode> iterator() {
    return new Iterator<OSHNode>() {
      private int pos = 0;

      @Override
      public OSHNode next() {
        int offset = index[pos];
        int length = (pos < index.length - 1 ? index[pos + 1] : data.length) - offset;
        pos++;
        try {
          return OSHNodeImpl.instance(data, offset, length, baseId, baseTimestamp, baseLongitude,
              baseLatitude);
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
    return String.format("Grid-Cell of OSHNodes %s", super.toString());
  }
}
