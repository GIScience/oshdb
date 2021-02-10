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
    final List<OSHDBTimestamp> allModificationTimestamps =
        getModificationTimestamps(osh, true, osmEntityFilter);

    if (allModificationTimestamps.size() <= 1) {
      return allModificationTimestamps;
    }
    // group modification timestamps by changeset
    long nextChangeset = -1L;
    int pos = allModificationTimestamps.size();
    for (OSHDBTimestamp timestamp : Lists.reverse(allModificationTimestamps)) {
      long changeset = changesetTimestamps.get(timestamp);
      if (changeset != nextChangeset) {
        allModificationTimestamps.set(--pos, timestamp);
      }
      nextChangeset = changeset;
    }

    return allModificationTimestamps.subList(pos, allModificationTimestamps.size());
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
    // first, store this ways direct modifications (i.e. the "major" versions' timestamps)
    var entityTimestamps = getModificationTimestampsReverseNonRecursed(osh, osmEntityFilter);
    if (!recurse || osh instanceof OSHNode) {
      return Lists.reverse(entityTimestamps);
    }
    // recurse members: start by collecting all referenced members' validity time intervals
    var membersValidityTimes = collectMembershipTimeIntervals(osh, osmEntityFilter);
    // fill in member modification timestamps
    SortedSet<OSHDBTimestamp> result = fillMembersModificationTimestamps(membersValidityTimes);
    // combine with the entity's own modification timestamps
    result.addAll(entityTimestamps);
    return new ArrayList<>(result);
  }

  /** Returns the filtered entity versions' timestamps in reverse order. */
  private static List<OSHDBTimestamp> getModificationTimestampsReverseNonRecursed(
      OSHEntity osh, Predicate<OSMEntity> osmEntityFilter) {
    List<OSHDBTimestamp> result = new ArrayList<>();
    // `nextNonMatchTime` stores the timestamp when the entity is deleted or doesn't match the
    // filter anymore. This is used to fold consecutive "non-matching" versions. example:
    //     a way has tag K=A in version 1, tag K=B in version 2 and is deleted in version 3, and
    //     the filter only matches versions with the tag K=A. Then in the first iteration of the
    //     loop, the nextNonMatchTime will be set to t3, in the second iteration it will be set
    //     to t2 and in the third iteration it will be added to the result.
    OSHDBTimestamp nextNonMatchTime = null;
    for (OSMEntity osm : osh.getVersions()) {
      if (osm.isVisible() && (osmEntityFilter == null || osmEntityFilter.test(osm))) {
        if (nextNonMatchTime != null) {
          // the next version of this entity is deleted or doesn't match the filter anymore
          // -> also return the time of the "deletion"
          result.add(nextNonMatchTime);
          nextNonMatchTime = null;
        }
        result.add(osm.getTimestamp());
      } else {
        // save the time of the next "deletion" for later
        nextNonMatchTime = osm.getTimestamp();
      }
    }
    return result;
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
      // skip versions which are deleted or don't match the given filter
      if (!osm.isVisible() || (osmEntityFilter != null && !osmEntityFilter.test(osm))) {
        // remember "valid-to" time
        nextT = thisT;
        continue;
      }
      for (OSMMember member : getRefsOrMembers(osm)) {
        switch (member.getType()) {
          case NODE:
          case WAY:
            OSHEntity oshEntity = member.getEntity();
            if (oshEntity == null) {
              continue;
            }
            LinkedList<OSHDBTimestamp> memberValidityTimestamps =
                memberTimes.computeIfAbsent(oshEntity, ignored -> new LinkedList<>());
            if (!memberValidityTimestamps.isEmpty()
                && memberValidityTimestamps.getFirst().equals(nextT)) {
              // merge consecutive time intervals
              memberValidityTimestamps.pop();
              memberValidityTimestamps.push(thisT);
            } else {
              // add new time interval for this member
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

  /** Returns the list of all members of a relation or referenced nodes of a way. */
  private static OSMMember[] getRefsOrMembers(OSMEntity osm) {
    switch (osm.getType()) {
      case RELATION:
        return ((OSMRelation) osm).getMembers();
      case WAY:
        return  ((OSMWay) osm).getRefs();
      default:
        final String illegalOSMTypeMessage
            = "cannot collect members from anything other than ways or relations";
        assert false : illegalOSMTypeMessage;
        throw new IllegalStateException(illegalOSMTypeMessage);
    }
  }

  /**
   * Returns the members' modification timestamps inside their given validity/membership time
   * intervals.
   */
  private static SortedSet<OSHDBTimestamp> fillMembersModificationTimestamps(
      Map<OSHEntity, LinkedList<OSHDBTimestamp>> membersValidityTimes) {
    SortedSet<OSHDBTimestamp> result = new TreeSet<>();
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
