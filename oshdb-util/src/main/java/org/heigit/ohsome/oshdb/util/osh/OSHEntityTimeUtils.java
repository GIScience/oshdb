package org.heigit.ohsome.oshdb.util.osh;

import static java.lang.String.format;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import org.heigit.ohsome.oshdb.osh.OSHEntities;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;

public class OSHEntityTimeUtils {
  private OSHEntityTimeUtils() {
    throw new IllegalStateException("utility class");
  }

  /**
   * Returns the changeset ids which correspond to modifications of this entity.
   *
   * <p>Used internally to group modifications by changeset.</p>
   *
   * @param osh the osh entity to work on
   * @return a map between timestamps and changeset ids
   */
  public static Map<OSHDBTimestamp, Long> getChangesetTimestamps(OSHEntity osh) {
    switch (osh.getType()) {
      case NODE:
        return getChangesetTimestamps((OSHNode) osh);
      case WAY:
        return getChangesetTimestamps((OSHWay) osh);
      case RELATION:
        return getChangesetTimestamps((OSHRelation) osh);
      default:
        throw new UnsupportedOperationException(format(UNSUPPORTED_OSMTYPE_MESSAGE, osh.getType()));
    }
  }

  private static Map<OSHDBTimestamp, Long> getChangesetTimestamps(OSHNode osh) {
    Map<OSHDBTimestamp, Long> result = new TreeMap<>();
    putChangesetTimestamps(osh, result);
    return result;
  }

  private static Map<OSHDBTimestamp, Long> getChangesetTimestamps(OSHWay osh) {
    Map<OSHDBTimestamp, Long> result = new TreeMap<>();
    putChangesetTimestamps(osh, result);
    // recurse way nodes
    try {
      putChangesetTimestamps(osh.getNodes(), result);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return result;
  }

  private static Map<OSHDBTimestamp, Long> getChangesetTimestamps(OSHRelation osh) {
    Map<OSHDBTimestamp, Long> result = new TreeMap<>();
    putChangesetTimestamps(osh, result);
    // recurse rel members
    try {
      putChangesetTimestamps(osh.getNodes(), result);
      putChangesetTimestampsRecurse(osh.getWays(), result);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return result;
  }

  /**
   * Returns all timestamps at which this entity (or one or more of its child entities) has been
   * modified.
   * 
   * @param osh the osh entity to work on
   * @return a list of timestamps where this entity has been modified
   */
  public static List<OSHDBTimestamp> getModificationTimestamps(OSHEntity osh) {
    return getModificationTimestamps(osh, true);
  }

  /**
   * Returns the list of timestamps at which this entity was modified.
   *
   * <p>
   * If the parameter "recurse" is set to true, it will also include modifications of the object's
   * child elements (useful to find out when the geometry of this object has been altered).
   * </p>
   *
   * @param osh the osh entity to work on
   * @param recurse specifies if times of modifications of child entities should also be returned or
   *        not
   * @return a list of timestamps where this entity has been modified
   */
  public static List<OSHDBTimestamp> getModificationTimestamps(OSHEntity osh, boolean recurse) {
    switch (osh.getType()) {
      case NODE:
        return getModificationTimestamps((OSHNode) osh);
      case WAY:
      case RELATION:
        return getModificationTimestamps(osh, recurse, null);
      default:
        throw new UnsupportedOperationException(format(UNSUPPORTED_OSMTYPE_MESSAGE, osh.getType()));
    }
  }

  private static List<OSHDBTimestamp> getModificationTimestamps(OSHNode osh) {
    return Lists.reverse(OSHEntities.toList(osh.getVersions(), OSMEntity::getTimestamp));
  }

  /**
   * Returns all timestamps at which this entity (or one or more of its child entities) has been
   * modified and matches a given condition/filter.
   *
   * <p>
   * Consecutive modifications from a single changeset are grouped together (only the last
   * modification timestamp of the corresponding changeset is considered). This can reduce the
   * amount of geometry modifications by a lot (e.g. when sequential node uploads of a way
   * modification causes many intermediate modification states), making results more
   * "accurate"/comparable as well as allowing faster processing of geometries.
   * </p>
   *
   * @param osh the osh entity to work on
   * @param osmEntityFilter only timestamps for which the entity matches this filter are returned
   * @param changesetTimestamps association between timestamps and changeset-ids, can be obtained
   *        from oshEntity by calling {@link #getChangesetTimestamps}.
   * @return a list of timestamps where this entity has been modified
   */
  public static List<OSHDBTimestamp> getModificationTimestamps(OSHEntity osh,
      Predicate<OSMEntity> osmEntityFilter, Map<OSHDBTimestamp, Long> changesetTimestamps) {
    List<OSHDBTimestamp> allModificationTimestamps =
        getModificationTimestamps(osh, true, osmEntityFilter);

    if (allModificationTimestamps.size() <= 1) {
      return allModificationTimestamps;
    }
    // group modification timestamps by changeset
    List<OSHDBTimestamp> result = new ArrayList<>(allModificationTimestamps.size());
    allModificationTimestamps = Lists.reverse(allModificationTimestamps);
    long nextChangeset = -1L;
    for (OSHDBTimestamp timestamp : allModificationTimestamps) {
      long changeset = changesetTimestamps.get(timestamp);
      if (changeset != nextChangeset) {
        result.add(timestamp);
      }
      nextChangeset = changeset;
    }

    return Lists.reverse(result);
  }

  /**
   * Returns all timestamps at which this entity (or one or more of its child entities) has been
   * modified and matches a given condition/filter.
   * 
   * @param osh the osh entity to work on
   * @param osmEntityFilter only timestamps for which the entity matches this filter are returned
   * @return a list of timestamps where this entity has been modified
   */
  public static List<OSHDBTimestamp> getModificationTimestamps(OSHEntity osh,
      Predicate<OSMEntity> osmEntityFilter) {
    return getModificationTimestamps(osh, true, osmEntityFilter);
  }

  private static List<OSHDBTimestamp> getModificationTimestamps(
      OSHEntity osh, boolean recurse, Predicate<OSMEntity> osmEntityFilter) {
    // first, store this ways direct modifications (i.e. the major versions' timestamps)
    var entityTimestamps = getModificationTimestampsReverseNonRecursed(osh, osmEntityFilter);
    if (!recurse || osh instanceof OSHNode) {
      return Lists.reverse(entityTimestamps);
    }
    // recurse members: start by collecting all referenced members' validity time intervals
    var membersValidityTimes = collectMembershipTimeIntervals(osh, osmEntityFilter);
    // fill in member modification timestamps
    var result = fillMembersModificationTimestamps(membersValidityTimes);
    // combine with the entity's own modification timestamps
    result.addAll(entityTimestamps);
    return new ArrayList<>(result);
  }

  /** Returns the filtered entity versions' timestamps in reverse order. */
  private static List<OSHDBTimestamp> getModificationTimestampsReverseNonRecursed(
      OSHEntity osh, Predicate<OSMEntity> osmEntityFilter) {
    List<OSHDBTimestamp> oshTs = new ArrayList<>();
    OSHDBTimestamp prevNonmatch = null;
    for (OSMEntity osm : osh.getVersions()) {
      if (osm.isVisible() && (osmEntityFilter == null || osmEntityFilter.test(osm))) {
        if (prevNonmatch != null) {
          oshTs.add(prevNonmatch);
          prevNonmatch = null;
        }
        oshTs.add(osm.getTimestamp());
      } else {
        prevNonmatch = osm.getTimestamp();
      }
    }
    return oshTs;
  }

  /**
   * Puts all changeset timestamps into the result of the given OSH entity.
   */
  private static void putChangesetTimestamps(OSHEntity osh, Map<OSHDBTimestamp, Long> result) {
    for (OSMEntity osm : osh.getVersions()) {
      result.putIfAbsent(osm.getTimestamp(), osm.getChangesetId());
    }
  }

  /**
   * Puts all changeset timestamps into the result of all OSH nodes in the given list.
   */
  private static void putChangesetTimestamps(
      List<OSHNode> oshList, Map<OSHDBTimestamp, Long> result) {
    for (OSHEntity osh : oshList) {
      if (osh == null) {
        continue;
      }
      for (OSMEntity osm : osh.getVersions()) {
        result.putIfAbsent(osm.getTimestamp(), osm.getChangesetId());
      }
    }
  }

  /**
   * Puts all changeset timestamps into the result of all OSH entities in the given list.
   */
  private static void putChangesetTimestampsRecurse(
      List<? extends OSHEntity> oshList, Map<OSHDBTimestamp, Long> result) {
    for (OSHEntity osh : oshList) {
      if (osh == null) {
        continue;
      }
      getChangesetTimestamps(osh).forEach(result::putIfAbsent);
    }
  }

  /**
   * Returns for each member of an entity a list of time intervals when the belong to the entity
   * matching the given predicate.
   */
  private static Map<OSHEntity, LinkedList<OSHDBTimestamp>> collectMembershipTimeIntervals(
      OSHEntity osh, Predicate<OSMEntity> osmEntityFilter) {
    Map<OSHEntity, LinkedList<OSHDBTimestamp>> memberTimes
        = new TreeMap<>(Comparator.comparingLong(OSHEntity::getId));
    OSHDBTimestamp nextT = new OSHDBTimestamp(Long.MAX_VALUE);
    for (OSMEntity osm : osh.getVersions()) {
      OSHDBTimestamp thisT = osm.getTimestamp();
      if (!osm.isVisible()
          || (osmEntityFilter != null && !osmEntityFilter.test(osm))) {
        nextT = thisT;
        continue;
      }
      OSMMember[] members;
      if (osm instanceof OSMRelation) {
        members = ((OSMRelation) osm).getMembers();
      } else if (osm instanceof OSMWay) {
        members = ((OSMWay) osm).getRefs();
      } else {
        final String illegalOSMTypeMessage
            = "cannot collect members from anything other than ways or relations";
        assert false : illegalOSMTypeMessage;
        throw new IllegalStateException(illegalOSMTypeMessage);
      }
      for (OSMMember member : members) {
        switch (member.getType()) {
          case NODE:
          case WAY:
            OSHEntity oshEntity = member.getEntity();
            if (oshEntity == null) {
              continue;
            }
            LinkedList<OSHDBTimestamp> memberValidityTimestamps;
            if (!memberTimes.containsKey(oshEntity)) {
              memberValidityTimestamps = new LinkedList<>();
              memberTimes.put(oshEntity, memberValidityTimestamps);
            } else {
              memberValidityTimestamps = memberTimes.get(oshEntity);
            }
            if (!memberValidityTimestamps.isEmpty()
                && memberValidityTimestamps.getFirst().equals(nextT)) {
              // merge consecutive time intervals
              memberValidityTimestamps.pop();
              memberValidityTimestamps.push(thisT);
            } else {
              memberValidityTimestamps.push(nextT);
              memberValidityTimestamps.push(thisT);
            }
            break;
          default:
          case RELATION:
            // skip relation->relation members
            break;
        }
      }
      nextT = thisT;
    }
    return memberTimes;
  }

  /**
   * Returns the members' modification timestamps inside their given validity/membership time
   * intervals.
   */
  private static SortedSet<OSHDBTimestamp> fillMembersModificationTimestamps(
      Map<OSHEntity, LinkedList<OSHDBTimestamp>> membersValidityTimes) {
    SortedSet<OSHDBTimestamp> result = new TreeSet<>();
    // iterate through all members individually
    for (var memberValidityTimes : membersValidityTimes.entrySet()) {
      var modTs = getModificationTimestamps(memberValidityTimes.getKey());
      if (modTs.isEmpty()) {
        // skip if the member has no "visible" version (for example because of data redactions)
        // see https://github.com/GIScience/oshdb/issues/325
        continue;
      }
      processSingleMember(modTs.iterator(), memberValidityTimes.getValue(), result);
    }
    return result;
  }

  /**
   * Processes a single member's modifaction timestamps and stores them in the "result" variable.
   *
   * @param modTs must not be empty
   * @param validMemberTs will be empty after this operation
   * @param result will be appended by this routine
   */
  private static void processSingleMember(
      Iterator<OSHDBTimestamp> modTs,
      LinkedList<OSHDBTimestamp> validMemberTs,
      SortedSet<OSHDBTimestamp> result
  ) {
    OSHDBTimestamp current = modTs.next();
    while (!validMemberTs.isEmpty()) {
      // fetch next membership validity time interval
      OSHDBTimestamp fromTs = validMemberTs.pop();
      OSHDBTimestamp toTs = validMemberTs.pop();
      // fast-forward until we find a timestamp which is after the start of the interval
      while (current.compareTo(fromTs) < 0) {
        if (!modTs.hasNext()) {
          // stop if there are no more modification timestamps to consider
          return;
        }
        current = modTs.next();
      }
      // add all of the member's modification timestamps which are in the interval to the result
      while (current.compareTo(toTs) <= 0) {
        result.add(current);
        if (!modTs.hasNext()) {
          // stop if there are no more modification timestamps to consider
          return;
        }
        current = modTs.next();
      }
    }
  }

  private static final String UNSUPPORTED_OSMTYPE_MESSAGE = "cannot get timestamps for %s objects";
}
