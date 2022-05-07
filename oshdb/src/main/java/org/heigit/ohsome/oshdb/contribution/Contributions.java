package org.heigit.ohsome.oshdb.contribution;

import static org.heigit.ohsome.oshdb.contribution.Contribution.major;
import static org.heigit.ohsome.oshdb.contribution.Contribution.minor;
import static org.heigit.ohsome.oshdb.osm.OSMType.NODE;
import static org.heigit.ohsome.oshdb.osm.OSMType.WAY;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.OSHDBIterator;

public abstract class Contributions extends OSHDBIterator<Contribution> {
  private static final Comparator<Contribution> CONTRIB_ORDER =
      Comparator.comparingLong(Contribution::getEpochSecond).reversed();
  private static final Comparator<Contributions> QUEUE_ORDER =
      Comparator.comparing(Contributions::peek, CONTRIB_ORDER);

  private static final EnumSet<ContributionType> CREATION = EnumSet.of(ContributionType.CREATION);


  public static Contributions of(OSHEntity osh) {
    return of(osh, Long.MAX_VALUE, x -> true);
  }

  public static Contributions of(OSHEntity osh, long maxTimestamp) {
    return of(osh, maxTimestamp, x -> true);
  }

  public static Contributions of(OSHEntity osh, long maxTimestamp, Predicate<OSMEntity> filter) {
    switch (osh.getType()) {
      case NODE:
        return new Nodes((OSHNode) osh, maxTimestamp, filter);
      case WAY:
        return new Ways((OSHWay) osh, maxTimestamp, filter);
      case RELATION:
        return new Rels((OSHRelation) osh, maxTimestamp, filter);
      default:
        throw new IllegalStateException();
    }
  }

  private abstract static class Entities<T extends OSMEntity> extends Contributions {
    private Predicate<OSMEntity> filter;
    protected OSHDBIterator<T> majorVersions;
    protected T major;
    protected long timestamp;
    protected long changeset;
    protected boolean visible;

    protected Entities(OSHDBIterator<T> majorVersions, long maxTimestamp,
        Predicate<OSMEntity> filter) {
      this.majorVersions = majorVersions;
      this.filter = filter;

      this.major = majorVersions.next();
      this.timestamp = maxTimestamp;
      this.changeset = -1;

      initMajor();
    }

    private void initMajor() {
      while (ts(major) > timestamp) {
        if (!majorVersions.hasNext()) {
          major = null;
          return;
        }
        major = majorVersions.next();
      }
      major = testOrAdvance(major);
      if (!filter.test(major)) {
        major = null;
      }
    }

    private T testOrAdvance(T osm) {
      visible = true;
      if (!test(osm)) {
        visible = false;
        while (majorVersions.hasNext() && !test(majorVersions.peek())) {
          osm = majorVersions.next();
        }
      }
      return osm;
    }

    protected boolean test(OSMEntity osm) {
      return osm != null && osm.isVisible() && filter.test(osm);
    }

    protected T getPrevMajor() {
      if (!majorVersions.hasNext()) {
        return null;
      }
      // squash changeset
      while (majorVersions.hasNext() && cs(majorVersions.peek()) == cs(major)) {
        majorVersions.next(); // skip
      }
      if (!majorVersions.hasNext()) {
        return null;
      }

      var osm = majorVersions.next();
      osm = testOrAdvance(osm);
      return osm;
    }

    protected EnumSet<ContributionType> checkTypes(OSMEntity osm, OSMEntity prev) {
      if (!test(prev)) {
        return CREATION;
      }
      if (!osm.getTags().equals(prev.getTags())) {
        return EnumSet.of(ContributionType.TAG_CHANGE);
      }
      return EnumSet.noneOf(ContributionType.class);
    }

    @Override
    protected Contribution getNext() {
      if (major == null) {
        return null;
      }
      if (!visible) {
        visible = true;
        timestamp = ts(major);
        changeset = cs(major);
        var contrib = Contribution.deletion(major);
        if (majorVersions.hasNext()) {
          major = majorVersions.next();
          initQueue(major);
        } else {
          major = null;
        }
        return contrib;
      }
      return nextContribution();
    }

    protected abstract Contribution nextContribution();

    protected void initQueue(T osm) {}

    protected Contribution nextMinorMajorContribution(Queue<Contributions> queue,
        List<Contributions> activeMembers) {
      advanceQueue(queue, timestamp, changeset);
      var members = initMembers(activeMembers);
      timestamp = -1;
      var minorTypes = EnumSet.noneOf(ContributionType.class);
      if (!queue.isEmpty() && (ts(queue) >= ts(major))) {
        var minorContribs = queue.poll();
        var minorContrib = minorContribs.next();
        timestamp = ts(minorContrib);
        changeset = cs(minorContrib);
        if (minorContribs.hasNext()) {
          queue.add(minorContribs);
        }
        minorTypes = EnumSet.copyOf(minorContrib.getTypes());
        minorTypes.addAll(minorContrib.getMinorTypes());
        // squash changeset
        squashChangesetMinor(queue, changeset, major, minorTypes);
        if (changeset != cs(major)) {
          return minor(timestamp, changeset, user(minorContrib), major, minorTypes, members);
        }
      }

      timestamp = Math.max(timestamp, ts(major));
      changeset = cs(major);

      var prev = getPrevMajor();
      var types = checkTypes(major, prev);
      if (types != CREATION) {
        checkMemberChange(mems(major), mems(prev), types);
      }
      if (types.contains(ContributionType.MEMBER_CHANGE)) {
        initQueue(prev);
      }
      var contrib = major(timestamp, major, types, minorTypes, members);
      major = prev;
      return contrib;
    }

    protected abstract OSMMember[] mems(T osm);
  }

  private static class Nodes extends Entities<OSMNode> {
    private Nodes(OSHNode osh, long maxTimestamp, Predicate<OSMEntity> filter) {
      super(OSHDBIterator.peeking(osh.getVersions()), maxTimestamp, filter);
    }

    @Override
    protected Contribution nextContribution() {
      var prev = getPrevMajor();
      var types = checkTypes(major, prev);
      if (types != CREATION && checkGeomChange(major, prev)) {
        types.add(ContributionType.GEOMETRY_CHANGE);
      }
      var contrib = Contribution.node(major, types);
      major = prev;
      return contrib;
    }

    private boolean checkGeomChange(OSMNode osm, OSMNode prev) {
      return osm.getLon() != prev.getLon() || osm.getLat() != prev.getLat();
    }

    @Override
    protected OSMMember[] mems(OSMNode osm) {
      throw new UnsupportedOperationException();
    }
  }

  private static final Queue<Contributions> EMPTY_QUEUE = new ArrayDeque<>(0);

  private static class Ways extends Entities<OSMWay> {
    private Queue<Contributions> queue = EMPTY_QUEUE;
    private List<Contributions> activeMembers;

    private Map<Long, Contributions> oshMembers;
    private Set<Long> active = Collections.emptySet();

    protected Ways(OSHWay osh, long maxTimestamp, Predicate<OSMEntity> filter) {
      super(OSHDBIterator.peeking(osh.getVersions()), maxTimestamp, filter);
      if (major != null) {
        this.oshMembers = members(osh.getNodes(), timestamp);
        if (!oshMembers.isEmpty()) {
          queue = new PriorityQueue<>(oshMembers.size(), QUEUE_ORDER);
        }
        if (visible) {
          initQueue(major);
        }

      }
    }

    @Override
    protected void initQueue(OSMWay osm) {
      var members = osm.getMembers();
      var newActive = Sets.<Long>newHashSetWithExpectedSize(members.length);
      var newActiveMembers = new ArrayList<Contributions>(members.length);
      for (var member : members) {
        var memId = member.getId();
        var contribs = oshMembers.get(memId);
        if (contribs != null) {
          newActiveMembers.add(contribs);
          // only add if not already in queue
          if (newActive.add(memId) && !active.remove(memId)) {
            queue.add(contribs);
          }
        }
      }
      // remove previous active from queue
      queue.removeIf(contribs -> !newActive.contains(contribs.peek().getId()));
      active = newActive;
      activeMembers = newActiveMembers;
    }

    @Override
    protected OSMMember[] mems(OSMWay osm) {
      return osm.getMembers();
    }

    @Override
    protected Contribution nextContribution() {
      return nextMinorMajorContribution(queue, activeMembers);
    }
  }

  private static class Rels extends Entities<OSMRelation> {
    private Queue<Contributions> queue = EMPTY_QUEUE;
    private List<Contributions> activeMembers;

    private Map<Long, Contributions> oshNodeMembers;
    private Map<Long, Contributions> oshWayMembers;

    private Set<Long> activeNodes = Collections.emptySet();
    private Set<Long> activeWays = Collections.emptySet();

    protected Rels(OSHRelation osh, long maxTimestamp, Predicate<OSMEntity> filter) {
      super(OSHDBIterator.peeking(osh.getVersions()), maxTimestamp, filter);

      if (major != null) {
        this.oshNodeMembers = members(osh.getNodes(), timestamp);
        this.oshWayMembers = members(osh.getWays(), timestamp);
        var queueSize = oshNodeMembers.size() + oshWayMembers.size();
        if (queueSize > 0) {
          queue = new PriorityQueue<>(queueSize, QUEUE_ORDER);
        }
        if (visible) {
          initQueue(major);
        }
      }
    }

    @Override
    protected void initQueue(OSMRelation osm) {
      var members = osm.getMembers();
      var newActiveNodes = !oshNodeMembers.isEmpty() ? new HashSet<Long>() : activeNodes;
      var newActiveWays = !oshWayMembers.isEmpty() ? new HashSet<Long>() : activeWays;
      var newActiveMembers = new ArrayList<Contributions>(members.length);
      for (var member : members) {
        var memType = member.getType();
        var memId = member.getId();

        var contribs = memberContribs(memType, memId);
        if (contribs != null && contribs.hasNext()) {
          newActiveMembers.add(contribs);
          // only add if not already in queue
          if ((NODE == memType && newActiveNodes.add(memId) && !activeNodes.remove(memId))
              || (WAY == memType && newActiveWays.add(memId) && !activeWays.remove(memId))) {
            queue.add(contribs);
          }
        }
      }
      // remove previous active from queue
      queue.removeIf(contribs -> {
        var contrib = contribs.peek();
        var memId = contrib.getId();
        switch (contrib.getType()) {
          case NODE:
            return !newActiveNodes.contains(memId);
          case WAY:
            return !newActiveWays.contains(memId);
          default:
            return true;
        }
      });
      activeNodes = newActiveNodes;
      activeWays = newActiveWays;
      activeMembers = newActiveMembers;
    }

    private Contributions memberContribs(OSMType type, long id) {
      switch (type) {
        case NODE:
          return oshNodeMembers.get(id);
        case WAY:
          return oshWayMembers.get(id);
        default:
          return null;
      }
    }

    @Override
    protected OSMMember[] mems(OSMRelation osm) {
      return osm.getMembers();
    }

    @Override
    protected Contribution nextContribution() {
      return nextMinorMajorContribution(queue, activeMembers);
    }
  }

  protected Map<Long, Contributions> members(List<? extends OSHEntity> members, long timestamp) {
    var map = Maps.<Long, Contributions>newHashMapWithExpectedSize(members.size());
    for (var member : members) {
      map.put(member.getId(), Contributions.of(member, timestamp));
    }
    return map;
  }

  protected void advanceQueue(Queue<Contributions> queue, long timestamp, long changeset) {
    while (!queue.isEmpty() && (ts(queue) > timestamp || cs(queue) == changeset)) {
      var contribs = queue.poll();
      contribs.next(); // skip
      if (contribs.hasNext()) {
        queue.add(contribs);
      }
    }
  }

  protected List<Contribution> initMembers(List<Contributions> activeMembers) {
    var members = new ArrayList<Contribution>(activeMembers.size());
    for (var m : activeMembers) {
      if (m.hasNext()) {
        members.add(m.peek());
      } else {
        members.add(null);
      }
    }
    return members;
  }

  protected void squashChangesetMinor(Queue<Contributions> queue, long changeset, OSMEntity major,
      EnumSet<ContributionType> types) {
    while (!queue.isEmpty() && cs(queue) == changeset && ts(queue) >= ts(major)) {
      var contribs = queue.poll();
      var contrib = contribs.next(); // skip
      types.addAll(contrib.getTypes());
      types.addAll(contrib.getMinorTypes());
      if (contribs.hasNext()) {
        queue.add(contribs);
      }
    }
  }

  protected void checkMemberChange(OSMMember[] a, OSMMember[] b, EnumSet<ContributionType> types) {
    if (a == b) {
      return;
    }

    if (a.length != b.length) {
      types.add(ContributionType.MEMBER_CHANGE);
      return;
    }

    // IMPROVE: we could vectorize this
    for (int i = 0; i < a.length; i++) {
      if ((a[i].getType() != b[i].getType()) || (a[i].getId() != b[i].getId())) {
        types.add(ContributionType.MEMBER_CHANGE);
        return;
      }
      if (!a[i].getRole().equals(b[i].getRole())) {
        types.add(ContributionType.ROLE_CHANGE);
      }
    }
  }

  protected long ts(Queue<Contributions> queue) {
    return ts(queue.peek());
  }

  protected long ts(Contributions contribs) {
    return ts(contribs.peek());
  }

  protected long ts(Contribution contrib) {
    return contrib.getEpochSecond();
  }

  protected long ts(OSMEntity osm) {
    return osm.getEpochSecond();
  }

  protected long cs(Queue<Contributions> queue) {
    return cs(queue.peek());
  }

  protected long cs(Contributions contribs) {
    return cs(contribs.peek());
  }

  protected long cs(Contribution contrib) {
    return contrib.getChangeset();
  }

  protected long cs(OSMEntity osm) {
    return osm.getChangesetId();
  }

  protected int user(Contribution contrib) {
    return contrib.getUser();
  }
}
