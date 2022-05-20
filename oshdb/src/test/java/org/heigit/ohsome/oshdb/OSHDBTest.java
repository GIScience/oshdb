package org.heigit.ohsome.oshdb;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.DELETION;
import static org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl.build;
import static org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl.build;
import static org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl.build;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongFunction;
import org.heigit.ohsome.oshdb.contribution.Contribution;
import org.heigit.ohsome.oshdb.contribution.ContributionType;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;

/**
 * Helper Class for oshdb tests.
 */

public abstract class OSHDBTest {
  protected static final OSMMember[] NO_MEMBERS = new OSMMember[0];
  protected static final Set<ContributionType> NO_TYPES = EnumSet.noneOf(ContributionType.class);


  public void assertContrib(long time, long cs, int user, OSMEntity entity,
      Contribution contrib) {
    assertEquals(time, contrib.getEpochSecond());
    assertEquals(cs, contrib.getChangeset());
    assertEquals(user, contrib.getUser());
    assertEquals(entity, contrib.getEntity());
    assertMembersSize(entity, contrib);
  }



  public void assertMembersSize(OSMEntity entity, Contribution contrib) {
    if (contrib.getTypes().contains(DELETION)) {
      return;
    }
    var members = NO_MEMBERS;
    if (OSMType.WAY == entity.getType()) {
      members = ((OSMWay) entity).getMembers();
    } else if (OSMType.RELATION == entity.getType()) {
      members = ((OSMRelation) entity).getMembers();
    }
    assertEquals(members.length, contrib.getMembers().size());
  }



  @SafeVarargs
  public static OSHNode osh(long id, LongFunction<OSMNode>... versions) {
    return build(nodes(id, versions));
  }

  public static OSHNode osh(List<OSMNode> versions) {
    return build(versions);
  }

  @SafeVarargs
  public static OSHWay osh(long id, Collection<OSHNode> nodes,
      LongFunction<OSMWay>... versions) {
    return build(ways(id, versions), nodes);
  }

  public static OSHWay osh(List<OSMWay> versions, Collection<OSHNode> nodes) {
    return build(versions, nodes);
  }

  @SafeVarargs
  public static OSHRelation osh(long id, Collection<OSHNode> nodes, Collection<OSHWay> ways,
      LongFunction<OSMRelation>... versions) {
    return build(relations(id, versions), nodes, ways);
  }

  public static OSHRelation osh(List<OSMRelation> versions, Collection<OSHNode> nodes,
      Collection<OSHWay> ways) {
    return build(versions, nodes, ways);
  }

  @SafeVarargs
  public static List<OSMNode> nodes(long id, LongFunction<OSMNode>... versions) {
    return stream(versions).map(o -> o.apply(id)).collect(toList());
  }

  @SafeVarargs
  public static List<OSMWay> ways(long id, LongFunction<OSMWay>... versions) {
    return stream(versions).map(o -> o.apply(id)).collect(toList());
  }

  @SafeVarargs
  public static List<OSMRelation> relations(long id, LongFunction<OSMRelation>... versions) {
    return stream(versions).map(o -> o.apply(id)).collect(toList());
  }

  public static LongFunction<OSMNode> node(int version, long timestamp, long changeset,
      int userId, int[] tags, int lon, int lat) {
    return (id) -> OSM.node(id, version, timestamp, changeset, userId, tags, lon, lat);
  }

  public static LongFunction<OSMWay> way(int version, long timestamp, long changeset, int userId,
      int[] tags, OSMMember[] members) {
    return (id) -> OSM.way(id, version, timestamp, changeset, userId, tags, members);
  }

  public static LongFunction<OSMRelation> relation(int version, long timestamp, long changeset,
      int userId, int[] tags, OSMMember[] members) {
    return (id) -> OSM.relation(id, version, timestamp, changeset, userId, tags, members);
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

  public static OSMMember[] mems() {
    return NO_MEMBERS;
  }

  public static OSMMember[] mems(long... ids) {
    var members = new OSMMember[ids.length];
    for (int i = 0; i < ids.length; i++) {
      members[i] = n(ids[i]);
    }
    return members;
  }

  public static OSMMember[] mems(OSMMember... members) {
    return members;
  }

  public static OSMMember m(OSMType type, long id, int role) {
    return new OSMMember(id, type, role);
  }

  public static OSMMember n(long id) {
    return m(OSMType.NODE, id, -1);
  }

  public static OSMMember n(long id, int role) {
    return m(OSMType.NODE, id, role);
  }

  public static OSMMember w(long id, int role) {
    return m(OSMType.WAY, id, role);
  }

  public static OSMMember r(long id, int role) {
    return m(OSMType.RELATION, id, role);
  }
}
