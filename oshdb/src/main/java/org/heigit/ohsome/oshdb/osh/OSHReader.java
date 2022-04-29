package org.heigit.ohsome.oshdb.osh;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxOSMCoordinates;
import static org.heigit.ohsome.oshdb.util.BitUtil.checkBit;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.heigit.ohsome.oshdb.OSHDBBoundable;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBMembers;
import org.heigit.ohsome.oshdb.OSHDBTags;
import org.heigit.ohsome.oshdb.io.OSHDBInput;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMembers;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;

@SuppressWarnings("javadoc")
public class OSHReader {

  public interface OSHMemberReader {
    OSHDBMembers readWayMembers();
    OSHDBMembers readRelationMembers();
  }

  public interface OSHTagReader {
    OSHDBTags readTags();
  }

  private static final OSHDBBoundingBox ZERO_BASE = bboxOSMCoordinates(0, 0, 0, 0);

  public static final int SINGLE = 1 << 0;
  public static final int LIVE = 1 << 1;
  public static final int TAGS = 1 << 2;

  public static final int CHG_VERSION = 1 << 0;
  public static final int CHG_VISIBLE = 1 << 1;
  public static final int CHG_USER = 1 << 2;
  public static final int CHG_TAGS = 1 << 3;
  public static final int CHG_EXT = 1 << 4;


  private OSHTagReader oshTagReader;
  private OSHMemberReader oshMemberReader;
  private OSHDBBoundable reference = ZERO_BASE;

  public OSHReader(OSHTagReader tagReader, OSHMemberReader memberReader) {
    this(tagReader, memberReader, ZERO_BASE);
  }

  public OSHReader(OSHTagReader tagReader, OSHMemberReader memberReader, OSHDBBoundable reference) {
    this.oshTagReader = tagReader;
    this.oshMemberReader = memberReader;
    this.reference = reference;
  }

  public OSHNode readNode(long id, OSHDBInput in) {
    var header = in.readByte();
    var bbox = bbox(in, checkBit(header, SINGLE));
    var tags = checkBit(header, TAGS) ? oshTagReader.readTags() : OSHDBTags.empty();
    return readNode(id, bbox, tags, header, in);
  }

  public OSHNode readNode(long id, OSHDBBoundable bbox, OSHDBTags tags, int header, OSHDBInput in) {
    var minLon = bbox.getMinLongitude();
    var minLat = bbox.getMinLatitude();
    Iterable<OSMNode> versions;
    if (checkBit(header, SINGLE)) {
      var timestamp = in.readUInt64();
      var changeset = in.readUInt64();
      var user = in.readUInt32();
      List<OSMNode> list;
      if (!checkBit(header, LIVE)) {
        list = new ArrayList<>(2);
        list.add(node(id, 2, false, timestamp, changeset, user, tags, minLon, minLat));
        timestamp += in.readSInt64();
        changeset += in.readSInt64();
        user += in.readSInt32();
      } else {
        list = new ArrayList<>(1);
      }
      list.add(node(id, 1, true, timestamp, changeset, user, tags, minLon, minLat));
      versions = list;
    } else {
      versions = () -> new NodeVersions(id, in, tags, checkBit(header, LIVE), minLon, minLat);
    }
    return new Node(id, bbox, tags, versions);
  }

  public OSHWay readWay(long id, OSHDBInput in) {
    var header = in.readByte();
    var bbox = bbox(in, false);
    var tags = checkBit(header, TAGS) ? oshTagReader.readTags() : OSHDBTags.empty();
    var members = oshMemberReader.readWayMembers();
    return readWay(id, bbox, tags, members, header, in);
  }

  public OSHWay readWay(long id, OSHDBBoundable bbox, OSHDBTags tags, OSHDBMembers members,
      int header, OSHDBInput in) {
    Iterable<OSMWay> versions;
    if (checkBit(header, SINGLE)) {
      var timestamp = in.readUInt64();
      var changeset = in.readUInt64();
      var user = in.readUInt32();
      var mems = members.members(readMembers(in));
      List<OSMWay> list;
      if (!checkBit(header, LIVE)) {
        list = new ArrayList<>(2);
        list.add(way(id, 2, false, timestamp, changeset, user, tags, mems));
        timestamp += in.readSInt64();
        changeset += in.readSInt64();
        user += in.readSInt32();
      } else {
        list = new ArrayList<>(1);
      }
      list.add(way(id, 1, true, timestamp, changeset, user, tags, mems));
      versions = list;
    } else {
      versions = () -> new WayVersions(id, in, tags, members, checkBit(header, LIVE));
    }
    return new Way(id, bbox, tags, members, versions);
  }

  public OSHRelation readRelation(long id, OSHDBInput in) {
    var header = in.readByte();
    var bbox = bbox(in, false);
    var tags = checkBit(header, TAGS) ? oshTagReader.readTags() : OSHDBTags.empty();
    var members = oshMemberReader.readRelationMembers();
    return readRelation(id, bbox, tags, members, header, in);
  }

  public OSHRelation readRelation(long id, OSHDBBoundable bbox, OSHDBTags tags,
      OSHDBMembers members, int header, OSHDBInput in) {
    Iterable<OSMRelation> versions;
    if (checkBit(header, SINGLE)) {
      var timestamp = in.readUInt64();
      var changeset = in.readUInt64();
      var user = in.readUInt32();
      var mems = members.members(readMembers(in), readRoles(in));
      List<OSMRelation> list;
      if (!checkBit(header, LIVE)) {
        list = new ArrayList<>(2);
        list.add(relation(id, 2, false, timestamp, changeset, user, tags, mems));
        changeset = in.readSInt64();
        user = in.readSInt32();
      } else {
        list = new ArrayList<>(1);
      }
      list.add(relation(id, 1, true, timestamp, changeset, user, tags, mems));
      versions = list;
    } else {
      versions = () -> new RelationVersions(id, in, tags, members, checkBit(header, LIVE));
    }
    return new Relation(id, bbox, tags, members, versions);
  }

  private static int[] readTags(OSHDBInput in, OSHDBTags tags) {
    return null;
  }

  private static int[] readMembers(OSHDBInput in) {
    return null;
  }

  private static int[] readRoles(OSHDBInput in) {
    return null;
  }

  private OSHDBBoundable bbox(OSHDBInput in, boolean point) {
    var minLon = (int) (reference.getMinLongitude() + in.readSInt64());
    var minLat = reference.getMinLatitude() + in.readSInt32();
    var maxLon = point ? minLon : (int) (minLon + in.readUInt64());
    var maxLat = point ? minLat : minLat + in.readUInt32();
    return bboxOSMCoordinates(minLon, minLat, maxLon, maxLat);
  }

  private abstract static class EntityVersions<T extends OSMEntity> implements Iterator<T> {
    protected final long id;
    protected final OSHDBInput in;
    protected final OSHDBTags oshTags;

    protected int version;
    protected boolean visible;

    protected long timestamp;
    protected long changeset;
    protected int user;
    protected OSHDBTags tags;

    protected int header;

    private boolean hasNext;

    protected EntityVersions(long id, OSHDBInput in, OSHDBTags oshTags, boolean visible) {
      this.id = id;
      this.in = in;
      this.oshTags = oshTags;
      this.version = in.readUInt32();
      this.visible = visible;
      this.timestamp = in.readUInt64();
      this.changeset = in.readUInt64();
      this.user = in.readUInt32();
      this.tags = oshTags.view(readTags(in, oshTags));
      this.hasNext = true;
    }

    @Override
    public boolean hasNext() {
      return hasNext || (hasNext = nextEntity());
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return getNext();
    }

    protected abstract T getNext();

    private boolean nextEntity() {
      if (!in.hasRemaining()) {
        return false;
      }
      header = in.readByte();
      version -= checkBit(header, CHG_VERSION) ? in.readUInt32() : 1;
      visible = checkBit(header, CHG_VISIBLE);
      timestamp += in.readSInt64();
      changeset += in.readSInt64();
      if (checkBit(header, CHG_USER)) {
        user -= in.readSInt32();
      }
      if (checkBit(header, CHG_TAGS)) {
        tags = oshTags.view(readTags(in, tags));
      }
      return true;
    }
  }

  private static class NodeVersions extends EntityVersions<OSMNode> {
    private int lon;
    private int lat;

    public NodeVersions(long id, OSHDBInput in, OSHDBTags oshTags, boolean visible, int minLon,
        int minLat) {
      super(id, in, oshTags, visible);
      lon = (int) (minLon + in.readUInt64());
      lat = minLat + in.readUInt32();
    }

    public OSMNode getNext() {
      if (checkBit(header, CHG_EXT)) {
        lon = (int) (lon + in.readSInt64());
        lat = lat + in.readSInt32();
      }
      return node(id, version, visible, timestamp, changeset, user, tags, lon, lat);
    }
  }

  private static class WayVersions extends EntityVersions<OSMWay> {
    private final OSHDBMembers oshMembers;
    private OSMMembers members;

    private WayVersions(long id, OSHDBInput in, OSHDBTags oshTags, OSHDBMembers oshMembers,
        boolean visible) {
      super(id, in, oshTags, visible);
      this.oshMembers = oshMembers;
    }

    @Override
    public OSMWay getNext() {
      if (checkBit(header, CHG_EXT)) {
        var memIdx = readMembers(in);
        members = oshMembers.members(memIdx);
      }
      return way(id, version, visible, timestamp, changeset, user, tags, members);
    }
  }

  private static class RelationVersions extends EntityVersions<OSMRelation> {
    private final OSHDBMembers oshMembers;
    private OSMMembers members;

    private RelationVersions(long id, OSHDBInput in, OSHDBTags oshTags, OSHDBMembers oshMembers,
        boolean visible) {
      super(id, in, oshTags, visible);
      this.oshMembers = oshMembers;
    }

    @Override
    public OSMRelation getNext() {
      if (checkBit(header, CHG_EXT)) {
        var memIdx = readMembers(in);
        var roleIdx = readRoles(in);
        members = oshMembers.members(memIdx, roleIdx);
      }
      return relation(id, version, visible, timestamp, changeset, user, tags, members);
    }
  }


  private abstract static class Entity implements OSHEntity {
    private final long id;
    private final OSHDBBoundable bbox;
    private final OSHDBTags tags;

    public Entity(long id, OSHDBBoundable bbox, OSHDBTags tags) {
      this.id = id;
      this.bbox = bbox;
      this.tags = tags;
    }

    @Override
    public long getId() {
      return id;
    }

    @Override
    public OSHDBBoundable getBoundable() {
      return bbox;
    }

    @Override
    public Iterable<OSHDBTagKey> getTagKeys() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean hasTagKey(OSHDBTagKey tag) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean hasTagKey(int key) {
      // TODO Auto-generated method stub
      return false;
    }
  }

  private static class Node extends Entity implements OSHNode {
    private final Iterable<OSMNode> versions;

    private Node(long id, OSHDBBoundable bbox, OSHDBTags tags, Iterable<OSMNode> versions) {
      super(id, bbox, tags);
      this.versions = versions;
    }

    @Override
    public Iterable<OSMNode> getVersions() {
      return versions;
    }
  }

  private static class Way extends Entity implements OSHWay {
    private final Iterable<OSMWay> versions;
    private final OSHDBMembers members;

    private Way(long id, OSHDBBoundable bbox, OSHDBTags tags, OSHDBMembers members,
        Iterable<OSMWay> versions) {
      super(id, bbox, tags);
      this.versions = versions;
      this.members = members;
    }

    @Override
    public Iterable<OSMWay> getVersions() {
      return versions;
    }
  }

  private static class Relation extends Entity implements OSHRelation {
    private final Iterable<OSMRelation> versions;
    private final OSHDBMembers members;

    private Relation(long id, OSHDBBoundable bbox, OSHDBTags tags, OSHDBMembers members,
        Iterable<OSMRelation> versions) {
      super(id, bbox, tags);
      this.versions = versions;
      this.members = members;
    }

    @Override
    public Iterable<OSMRelation> getVersions() {
      return versions;
    }
  }

  public static OSMNode node(long id, int version, boolean visible, long timestamp, long changeset,
      int user, OSHDBTags tags, int longitude, int latitude) {
    throw new UnsupportedOperationException();
  }

  public static OSMWay way(long id, int version, boolean visible, long timestamp, long changeset,
      int user, OSHDBTags tags, OSMMembers members) {
    throw new UnsupportedOperationException();
  }

  public static OSMRelation relation(long id, int version, boolean visible, long timestamp,
      long changeset, int user, OSHDBTags tags, OSMMembers members) {
    throw new UnsupportedOperationException();
  }
}
