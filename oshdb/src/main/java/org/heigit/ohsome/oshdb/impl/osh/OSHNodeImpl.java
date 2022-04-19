package org.heigit.ohsome.oshdb.impl.osh;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayOutputWrapper;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayWrapper;

/**
 * An implementation of the {@link OSHNode} interface.
 */
public class OSHNodeImpl extends OSHEntityImpl implements OSHNode, Iterable<OSMNode>, Serializable {

  private static final long serialVersionUID = 1L;

  private static final int CHANGED_LOCATION = 1 << 2;
  private static final int HEADER_HAS_BOUNDINGBOX = 1 << 3;


  public static OSHNodeImpl instance(final byte[] data, final int offset, final int length) {
    return instance(data, offset, length, 0, 0, 0, 0);
  }

  /**
   * Creates an instances of {@code OSHNodeImpl} from the given byte array.
   */
  public static OSHNodeImpl instance(final byte[] data, final int offset, final int length,
      final long baseNodeId, final long baseTimestamp, final int baseLongitude,
      final int baseLatitude) {
    var wrapper = ByteArrayWrapper.newInstance(data, offset, length);
    var commonProps = new CommonEntityProps(data, offset, length);
    commonProps.setHeader(wrapper.readRawByte());
    if ((commonProps.getHeader() & HEADER_HAS_BOUNDINGBOX) != 0) {
      readBbox(wrapper, commonProps, baseLongitude, baseLatitude);
    } else {
      commonProps.setMinLon(1);
      commonProps.setMinLat(1);
      commonProps.setMaxLon(-1);
      commonProps.setMaxLat(-1);
    }
    readBaseAndKeys(wrapper, commonProps, baseNodeId, baseTimestamp, baseLongitude, baseLatitude);
    commonProps.setDataOffset(wrapper.getPos());
    commonProps.setDataLength(length - (commonProps.getDataOffset() - offset));
    return new OSHNodeImpl(commonProps);
  }

  private OSHNodeImpl(final CommonEntityProps p) {
    super(p);

    this.minLon = p.getMinLon();
    this.minLat = p.getMinLat();
    this.maxLon = p.getMaxLon();
    this.maxLat = p.getMaxLat();
    // correct bbox!
    if (!(minLon <= maxLon && minLat <= maxLat)) {
      minLon = Integer.MAX_VALUE;
      maxLon = Integer.MIN_VALUE;
      minLat = Integer.MAX_VALUE;
      maxLat = Integer.MIN_VALUE;
      for (OSMNode osm : this) {
        if (osm.isVisible()) {
          minLon = Math.min(minLon, osm.getLon());
          maxLon = Math.max(maxLon, osm.getLon());

          minLat = Math.min(minLat, osm.getLat());
          maxLat = Math.max(maxLat, osm.getLat());
        }
      }
    }
  }

  @Override
  public OSMType getType() {
    return OSMType.NODE;
  }

  @Override
  public Iterable<OSMNode> getVersions() {
    return this;
  }

  @Override
  public Iterator<OSMNode> iterator() {
    var wrapper = ByteArrayWrapper.newInstance(data, dataOffset, dataLength);
    return new VersionIterator(wrapper, id, baseTimestamp, baseLongitude, baseLatitude);
  }

  public static OSHNodeImpl build(List<OSMNode> versions) {
    return build(versions, 0, 0, 0, 0);
  }

  /**
   * Creates a {@code OSHNode} bases on the given list of node versions.
   */
  public static OSHNodeImpl build(List<OSMNode> versions, final long baseId,
      final long baseTimestamp, final int baseLongitude, final int baseLatitude) {
    ByteBuffer bb = buildRecord(versions, baseId, baseTimestamp, baseLongitude, baseLatitude);

    return OSHNodeImpl.instance(bb.array(), 0, bb.remaining(), baseId, baseTimestamp, baseLongitude,
        baseLatitude);
  }

  /**
   * Creates a {@code OSHNode} bases on the given list of node versions, but returns the underlying
   * ByteBuffer instead of an instance of {@code OSHNode}.
   */
  public static ByteBuffer buildRecord(List<OSMNode> versions, final long baseId,
      final long baseTimestamp, final int baseLongitude, final int baseLatitude) {
    Collections.sort(versions, VERSION_REVERSE_ORDER);

    int lastLongitude = baseLongitude;
    int lastLatitude = baseLatitude;

    int minLon = Integer.MAX_VALUE;
    int maxLon = Integer.MIN_VALUE;
    int minLat = Integer.MAX_VALUE;
    int maxLat = Integer.MIN_VALUE;

    ByteArrayOutputWrapper output = new ByteArrayOutputWrapper();
    Builder builder = new Builder(output, baseTimestamp);

    for (OSMNode node : versions) {
      OSMEntity version = node;

      byte changed = 0;

      if (version.isVisible()
          && (node.getLon() != lastLongitude || node.getLat() != lastLatitude)) {
        changed |= CHANGED_LOCATION;
      }
      builder.build(version, changed);
      if ((changed & CHANGED_LOCATION) != 0) {
        output.writeS32(node.getLon() - baseLongitude - (lastLongitude - baseLongitude));
        lastLongitude = node.getLon();
        output.writeS32(node.getLat() - baseLatitude - (lastLatitude - baseLatitude));
        lastLatitude = node.getLat();

        minLon = Math.min(minLon, lastLongitude);
        maxLon = Math.max(maxLon, lastLongitude);

        minLat = Math.min(minLat, lastLatitude);
        maxLat = Math.max(maxLat, lastLatitude);
      }
    } // for versions

    byte header = builder.getHeader(versions.size() > 1);
    if (minLon != maxLon || minLat != maxLat) {
      header |= HEADER_HAS_BOUNDINGBOX;
    }

    var buffer = builder.writeCommon(header, versions.get(0).getId() - baseId,
        (header & HEADER_HAS_BOUNDINGBOX) != 0,
        minLon - baseLongitude,
        minLat  - baseLatitude,
        maxLon, maxLat);

    buffer.writeByteArray(output.array(), 0, output.length());
    return ByteBuffer.wrap(buffer.array(), 0, buffer.length());
  }

  public boolean hasTags() {
    return (header & HEADER_HAS_TAGS) != 0;
  }

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private static class VersionIterator extends EntityVersionIterator<OSMNode> {
    private final int baseLongitude;
    private final int baseLatitude;
    private int longitude = 0;
    private int latitude = 0;

    private VersionIterator(ByteArrayWrapper wrapper, long id, long baseTimestamp,
        int baseLongitude, int baseLatitude) {
      super(wrapper, id, baseTimestamp);
      this.baseLongitude = baseLongitude;
      this.baseLatitude = baseLatitude;
    }

    @Override
    protected OSMNode extension(byte changed) {
      if ((changed & CHANGED_LOCATION) != 0) {
        longitude = wrapper.readS32() + longitude;
        latitude = wrapper.readS32() + latitude;
      }

      return OSM.node(id, version, baseTimestamp + timestamp, changeset, userId, keyValues,
          version > 0 ? baseLongitude + longitude : 0, version > 0 ? baseLatitude + latitude : 0);
    }
  }

  private static class SerializationProxy extends OSHEntitySerializationProxy {
    public SerializationProxy(OSHNodeImpl osh) {
      super(osh);
    }

    public SerializationProxy() {
      super(null);
    }

    @Override
    protected Object newInstance(byte[] data, long id, long baseTimestamp, int baseLongitude,
        int baseLatitude) {
      return OSHNodeImpl.instance(data, 0, data.length, id, baseTimestamp, baseLongitude,
          baseLatitude);
    }
  }

  @Override
  public String toString() {
    return String.format("OSHNode %s", super.toString());
  }
}
