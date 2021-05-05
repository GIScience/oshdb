package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayOutputWrapper;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayWrapper;

public abstract class OSHEntity2 {
  protected final byte[] data;

  protected final long baseTimestamp;
  protected final long baseLongitude;
  protected final long baseLatitude;

  protected final long id;
  protected final byte header;
  protected final OSHDBBoundingBox bbox;
  protected final int[] keys;
  protected final int dataOffset;
  protected final int dataLength;

  protected OSHEntity2(final byte[] data, final int offset, final int length, final byte header,
      final long id, final OSHDBBoundingBox bbox, final long baseTimestamp,
      final long baseLongitude, final long baseLatitude, final int[] keys, final int dataOffset,
      final int dataLength) {
    this.data = data;

    this.baseTimestamp = baseTimestamp;
    this.baseLongitude = baseLongitude;
    this.baseLatitude = baseLatitude;

    this.header = header;
    this.id = id;
    this.bbox = bbox;
    this.keys = keys;
    this.dataOffset = dataOffset;
    this.dataLength = dataLength;
  }

  public long getId() {
    return id;
  }

  public long getBaseTimestamp() {
    return baseTimestamp;
  }

  public long getBaseLongitude() {
    return baseLongitude;
  }

  public long getBaseLatitude() {
    return baseLatitude;
  }

  public abstract OSHBuilder builder();

  private static final int CHANGED_USER_ID = 1 << 0;
  private static final int CHANGED_TAGS = 1 << 1;
  private static final int CHANGED_EXTENSION = 1 << 2;

  protected abstract static class OSHBuilder {
    private Set<Integer> keySet = new TreeSet<>();

    public void build(ByteArrayOutputWrapper out, List<OSMEntity> versions, long baseTimestamp,
        long baseLongitude, long baseLatitude, Map<Long, Integer> nodeOffsets,
        Map<Long, Integer> wayOffsets, Map<Long, Integer> relationOffsets) throws IOException {
      ByteArrayOutputWrapper aux = new ByteArrayOutputWrapper();
      build(out, aux, versions, baseTimestamp, baseLongitude, baseLatitude, nodeOffsets, wayOffsets,
          relationOffsets);
    }

    public void build(ByteArrayOutputWrapper out, ByteArrayOutputWrapper aux,
        List<? extends OSMEntity> versions, long baseTimestamp, long baseLongitude,
        long baseLatitude, Map<Long, Integer> nodeOffsets, Map<Long, Integer> wayOffsets,
        Map<Long, Integer> relationOffsets) throws IOException {
      int versionNumber = 0;
      long timestamp = 0;
      long changeset = 0;
      int userId = -1;
      int[] tags = new int[0];

      for (OSMEntity version : versions) {
        final int visible = version.isVisible() ? 1 : -1;

        versionNumber = out.writeS32Delta(version.getVersion() * visible, versionNumber);
        timestamp = out.writeS64Delta(version.getEpochSecond(), timestamp);
        changeset = out.writeS64Delta(version.getChangesetId(), changeset);

        byte changed = 0;
        aux.reset();
        if (visible == 1) {
          if (version.getUserId() != userId) {
            changed |= CHANGED_USER_ID;
            userId = aux.writeS32Delta(version.getUserId(), userId);
          }
          if (!Arrays.equals(version.getRawTags(), tags)) {
            changed |= CHANGED_TAGS;
            tags = version.getRawTags();
            aux.writeU32(tags.length);
            for (int i = 0; i < tags.length; i++) {
              aux.writeU32(tags[i]);
              if (i % 2 == 0) {
                keySet.add(Integer.valueOf(tags[i]));
              }
            }
          }
          if (extension(aux, version, baseLongitude, baseLatitude, nodeOffsets, wayOffsets,
              relationOffsets)) {
            changed |= CHANGED_EXTENSION;
          }
        }

        out.writeByte(changed);
        out.writeByteArray(aux.array(), 0, aux.length());
      }
    }

    public Set<Integer> getKeySet() {
      return keySet;
    }

    protected abstract boolean extension(ByteArrayOutputWrapper out, OSMEntity version,
        long baseLongitude, long baseLatitude, Map<Long, Integer> nodeOffsets,
        Map<Long, Integer> wayOffsets, Map<Long, Integer> relationOffsets) throws IOException;
  }

  protected abstract static class OSMIterator<T> implements Iterator<T> {

    protected final ByteArrayWrapper in;
    protected final OSHEntity2 entity;

    protected int version = 0;
    protected long timestamp = 0;
    protected long changeset = 0;
    protected byte changed = 0;
    protected int userId = 0;
    protected int[] keyValues = new int[0];

    public OSMIterator(byte[] data, int offset, int length, OSHEntity2 entity) {
      this.in = ByteArrayWrapper.newInstance(data, offset, length);
      this.entity = entity;
    }

    @Override
    public boolean hasNext() {
      return in.hasLeft() > 0;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        version = in.readS32Delta(version);
        timestamp = in.readS64Delta(timestamp);
        changeset = in.readS64Delta(changeset);

        changed = in.readRawByte();

        if ((changed & CHANGED_USER_ID) != 0) {
          userId = in.readS32() + userId;
        }

        if ((changed & CHANGED_TAGS) != 0) {
          int size = in.readU32();
          keyValues = new int[size];
          for (int i = 0; i < size; i++) {
            keyValues[i] = in.readU32();
          }
        }

        return extension();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    protected boolean changedExtension() {
      return (changed & CHANGED_EXTENSION) != 0;
    }

    protected abstract T extension();
  }
}
