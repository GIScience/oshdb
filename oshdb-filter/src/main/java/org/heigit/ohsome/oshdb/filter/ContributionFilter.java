package org.heigit.ohsome.oshdb.filter;

import static java.util.stream.Collectors.joining;

import com.google.common.collect.Streams;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;

public class ContributionFilter implements FilterExpression {

  /**
   * A filter which selects OSM contributions by matching to a list of changeset ids.
   */
  static FilterExpression changesetIdEquals(long changesetId) {
    return new ChangesetIdFilter(
        "" + changesetId,
        osm -> osm.getChangesetId() == changesetId,
        contrib -> contrib.getChangesetId() == changesetId);
  }

  /**
   * A filter which selects OSM contributions by matching to a list of changeset ids.
   */
  static FilterExpression changesetIdEqualsAnyOf(Collection<Long> changesetIdList) {
    final Set<Long> changesetIds = new HashSet<>(changesetIdList);
    return new ChangesetIdFilter(
         changesetIdList.stream().map(Object::toString).collect(joining(",", "in(", ")")),
         osm -> changesetIds.contains(osm.getChangesetId()),
         contrib -> changesetIds.contains(contrib.getChangesetId()));
  }

  /**
   * A filter which selects OSM contributions by matching to a range of changeset ids.
   */
  static FilterExpression changesetIdRange(IdRange changesetIdRange) {
    return new ChangesetIdFilter(
        "in-range" + changesetIdRange,
        osm -> changesetIdRange.test(osm.getChangesetId()),
        contrib -> changesetIdRange.test(contrib.getChangesetId()));
  }

  static class ChangesetIdFilter extends ContributionFilter {
    private ChangesetIdFilter(String expression, Predicate<OSMEntity> osmRecursively,
        Predicate<OSMContribution> osmContribution) {
      super("changeset:" + expression, osmRecursively, osmContribution, false);
    }
  }

  /**
   * A filter which selects OSM contributions by a user id.
   */
  static FilterExpression contributorUserIdEquals(long userId) {
    return new ContributionFilter(
        "contributor:" + userId,
        osm -> osm.getUserId() == userId,
        contrib -> contrib.getContributorUserId() == userId, false);
  }

  /**
   * A filter which selects OSM contributions by matching to a list of contributor user ids.
   */
  static FilterExpression contributorUserIdEqualsAnyOf(Collection<Integer> contributorUserIdList) {
    final Set<Integer> contributorUserIds = new TreeSet<>(contributorUserIdList);
    return new ContributionFilter(
        "contributor:" + contributorUserIds.stream().map(Object::toString).collect(
            joining(",", "in(", ")")),
        osm -> contributorUserIds.contains(osm.getUserId()),
        contrib -> contributorUserIdList.contains(contrib.getContributorUserId()), false);
  }

  /**
   * A filter which selects OSM contributions by a range of contributor user ids.
   */
  static FilterExpression contributorUserIdRange(IdRange contributorUserIdRange) {
    return new ContributionFilter(
        "contributor:in-range" + contributorUserIdRange,
        osm -> contributorUserIdRange.test(osm.getUserId()),
        contrib -> contributorUserIdRange.test(contrib.getContributorUserId()), false);
  }

  private final String expression;
  private final Predicate<OSMEntity> osmRecursively;
  private final Predicate<OSMContribution> osmContribution;
  private final boolean negated;

  private ContributionFilter(String expression, Predicate<OSMEntity> osmRecursively,
      Predicate<OSMContribution> osmContribution, boolean negated) {
    this.expression = expression;
    this.osmRecursively = osmRecursively;
    this.osmContribution = osmContribution;
    this.negated = negated;
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return applyToOSHEntityRecursively(entity).anyMatch(osm -> osmRecursively.test(osm) ^ negated);
  }

  @Override
  public boolean applyOSMContribution(OSMContribution contribution) {
    return osmContribution.test(contribution) ^ negated;
  }

  @Override
  public String toString() {
    return (negated ? "not " : "") + expression;
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return true;
  }

  @Override
  public FilterExpression negate() {
    return new ContributionFilter(expression, osmRecursively, osmContribution, !negated);
  }

  @Override
  public boolean applyOSMEntitySnapshot(OSMEntitySnapshot ignored) {
    throw new IllegalStateException("contributor filter is not applicable to entity snapshots");
  }

  /**
   * Helper method to get a stream of all versions of an OSH entity, including its
   * references/members and references of members.
   *
   * @param entity the OSH entity to test.
   * @return Stream of all versions of this OSH and members and members members.
   */
  protected static Stream<OSMEntity> applyToOSHEntityRecursively(OSHEntity entity) {
    return Streams.concat(
        Streams.stream(entity.getVersions()),
        nodes(entity).flatMap(n -> Streams.stream(n.getVersions())),
        ways(entity).flatMap(w -> Streams.stream(w.getVersions())),
        ways(entity).flatMap(w -> nodes(w).flatMap(wn -> Streams.stream(wn.getVersions())))
    );
  }

  private static Stream<OSHNode> nodes(OSHEntity osh) {
    try {
      return osh.getNodes().stream();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Stream<OSHWay> ways(OSHEntity osh) {
    try {
      return osh.getWays().stream();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
