package org.heigit.ohsome.oshdb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;

/**
 * Utility/Helper class for OSHDB test cases.
 */
public class OSHDBTestHelper {
  private OSHDBTestHelper() {}

  public static OSHNode oshNode(List<OSMNode> versions) {
    return new ListOSHNode(versions, null);
  }


  private abstract static class ListOSHEntity<T extends OSMEntity> implements OSHEntity {
    private static final Comparator<OSMEntity> VERSION_ORDER =
        Comparator.comparingInt(OSMEntity::getVersion).reversed();
    protected final List<T> versions;
    protected final OSHDBBoundable bbox;
    protected final Set<OSHDBTag> tags;

    protected ListOSHEntity(List<T> versions, OSHDBBoundable bbox) {
      this.versions = new ArrayList<>(versions);
      this.bbox = bbox;

      this.versions.sort(VERSION_ORDER);
      this.tags = new TreeSet<>();
      this.versions.forEach(osm -> tags.addAll(osm.getTags()));
    }

    @Override
    public int getMinLongitude() {
      return bbox.getMinLongitude();
    }

    @Override
    public int getMinLatitude() {
      return bbox.getMinLatitude();
    }

    @Override
    public int getMaxLongitude() {
      return bbox.getMaxLongitude();
    }

    @Override
    public int getMaxLatitude() {
      return bbox.getMaxLatitude();
    }

    @Override
    public long getId() {
      return versions.get(0).getId();
    }

    @Override
    public Iterable<OSHDBTagKey> getTagKeys() {
      return () -> tags.stream().map(tag -> new OSHDBTagKey(tag.getKey())).iterator();
    }

    @Override
    public boolean hasTagKey(OSHDBTagKey tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasTagKey(int key) {
      throw new UnsupportedOperationException();
    }
  }

  private static class ListOSHNode extends ListOSHEntity<OSMNode> implements OSHNode {
    protected ListOSHNode(List<OSMNode> versions, OSHDBBoundable bbox) {
      super(versions, bbox);
    }

    @Override
    public Iterable<OSMNode> getVersions() {
      return versions;
    }
  }

  public static OSHDBTag tag(int k, int v) {
    return new OSHDBTag(k, v);
  }

  public static int[] tags(OSHDBTag... tags) {
    var kvs = new int[tags.length * 2];
    for (int i = 0; i < tags.length; i++) {
      kvs[i * 2 + 0] = tags[i].getKey();
      kvs[i * 2 + 1] = tags[i].getValue();
    }
    return kvs;
  }
}
