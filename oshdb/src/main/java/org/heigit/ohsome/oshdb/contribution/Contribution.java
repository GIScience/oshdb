package org.heigit.ohsome.oshdb.contribution;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.heigit.ohsome.oshdb.OSHDBTemporal;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMType;

public abstract class Contribution implements OSHDBTemporal {
  private static final Set<ContributionType> EMPTY_TYPES = EnumSet.noneOf(ContributionType.class);
  private static final EnumSet<ContributionType> DELETION = EnumSet.of(ContributionType.DELETION);

  public static Contribution deletion(OSMEntity entity) {
    if (OSMType.NODE.equals(entity.getType())) {
      return new NodeContribution((OSMNode) entity, DELETION);
    }
    return new EntityContribution(
        entity.getEpochSecond(),
        entity.getChangesetId(),
        entity.getUserId(), entity,
        DELETION, EMPTY_TYPES, Collections.emptyList());
  }

  public static Contribution node(OSMNode node, Set<ContributionType> types) {
    return new NodeContribution(node, types);
  }

  public static Contribution minor(long timestamp, long changeset, int user, OSMEntity entity,
      Set<ContributionType> types, List<Contribution> members) {
    return new EntityContribution(timestamp, changeset, user, entity,
        types, EMPTY_TYPES, members);
  }

  public static Contribution major(long timestamp, OSMEntity entity,
      Set<ContributionType> types, Set<ContributionType> minorTypes,
      List<Contribution> members) {
    return new EntityContribution(timestamp, entity.getChangesetId(), entity.getUserId(), entity,
        types, minorTypes, members);
  }

  private final OSMEntity entity;
  private final Set<ContributionType> types;

  private Contribution(OSMEntity entity, Set<ContributionType> types) {
    this.entity = entity;
    this.types = types;
  }

  @Override
  public long getEpochSecond() {
    return entity.getEpochSecond();
  }

  public long getChangeset() {
    return entity.getChangesetId();
  }

  public int getUser() {
    return entity.getUserId();
  }

  public OSMType getType() {
    return entity.getType();
  }

  public long getId() {
    return entity.getId();
  }

  @SuppressWarnings("unchecked")
  public <T extends OSMEntity> T getEntity() {
    return (T) entity;
  }

  public Set<ContributionType> getTypes(){
    return types;
  }

  public Set<ContributionType> getMinorTypes() {
    return EMPTY_TYPES;
  }

  public List<Contribution> getMembers() {
    return Collections.emptyList();
  }

  private static class NodeContribution extends Contribution {
    private NodeContribution(OSMNode entity, Set<ContributionType> types) {
      super(entity, types);
    }

  }

  private static class EntityContribution extends Contribution {
    private final long timestamp;
    private final long changeset;
    private final int user;
    private final Set<ContributionType> minorTypes;
    private final List<Contribution> members;

    public EntityContribution(long timestamp, long changeset, int user, OSMEntity entity,
        Set<ContributionType> types, Set<ContributionType> minorTypes, List<Contribution> members) {
      super(entity, types);
      this.timestamp = timestamp;
      this.changeset = changeset;
      this.user = user;
      this.minorTypes = minorTypes;
      this.members = members;
    }

    @Override
    public long getEpochSecond() {
      return timestamp;
    }

    @Override
    public long getChangeset() {
      return changeset;
    }

    @Override
    public int getUser() {
      return user;
    }

    @Override
    public Set<ContributionType> getMinorTypes() {
      return minorTypes;
    }

    @Override
    public List<Contribution> getMembers() {
      return members;
    }
  }

  @Override
  public String toString() {
    return String.format("Contrib %s:"
        + " [timestamp=%s, changeset=%s, user=%s, types=%s, minorTypes=%s, members=%s, entity=%s]",
        getType(), getEpochSecond(), getChangeset(), getUser(), getTypes(), getMinorTypes(),
        getMembers(), getEntity());
  }
}
