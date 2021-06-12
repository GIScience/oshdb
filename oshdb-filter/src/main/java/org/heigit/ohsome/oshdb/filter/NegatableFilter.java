package org.heigit.ohsome.oshdb.filter;

import com.google.common.collect.Streams;
import java.io.IOException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.jetbrains.annotations.Contract;
import org.locationtech.jts.geom.Geometry;

/**
 * A filter which implements the negate method using a boolean flag.
 */
class NegatableFilter implements Filter {
  abstract static class FilterInternal implements Filter {
    @Override
    public FilterExpression negate() {
      throw new IllegalStateException("Invalid call of inner negate() on a negatable filter");
    }

    /** Inverse of {@link FilterExpression#applyOSH(OSHEntity)} */
    @Contract(pure = true)
    boolean applyOSHNegated(OSHEntity entity) {
      return true;
    }

    @Override
    public boolean applyOSM(OSMEntity entity) {
      return true;
    }

    /** Inverse of {@link FilterExpression#applyOSM(OSMEntity)} */
    @Contract(pure = true)
    boolean applyOSMNegated(OSMEntity entity) {
      return true;
    }

    /** Inverse of {@link FilterExpression#applyOSMGeometry(OSMEntity, Supplier)}. */
    @Contract(pure = true)
    boolean applyOSMGeometryNegated(OSMEntity entity, Supplier<Geometry> geometrySupplier) {
      // dummy implementation for basic filters: ignores the geometry, just looks at the OSM entity
      return applyOSMNegated(entity);
    }

    /**
     * Helper method to test a predicate on all versions of an OSH entity, including its
     * references/members and references of members.
     *
     * @param entity the OSH entity to test.
     * @param predicate the predicate to apply to each version of the entity
     * @return true if any of the versions of the entity or its referenced child entities matches
     *         the given predicate.
     */
    protected static boolean applyToOSHEntityRecursively(
        OSHEntity entity, Predicate<OSMEntity> predicate) {
      try {
        switch (entity.getType()) {
          case NODE:
            return Streams.stream(entity.getVersions()).anyMatch(predicate);
          case WAY:
            return Streams.concat(
                Streams.stream(entity.getVersions()),
                entity.getNodes().stream().flatMap(n -> Streams.stream(n.getVersions()))
            ).anyMatch(predicate);
          case RELATION:
          default:
            return Streams.concat(
                Streams.stream(entity.getVersions()),
                entity.getNodes().stream().flatMap(n -> Streams.stream(n.getVersions())),
                entity.getWays().stream().flatMap(w -> Streams.stream(w.getVersions())),
                entity.getWays().stream().flatMap(w -> {
                  try {
                    return w.getNodes().stream().flatMap(wn -> Streams.stream(wn.getVersions()));
                  } catch (IOException ignored) {
                    return Stream.<OSMEntity>empty();
                  }
                })
            ).anyMatch(predicate);
        }
      } catch (IOException ignored) {
        return true;
      }
    }
  }

  private final boolean negated;
  final FilterInternal filter;

  private NegatableFilter(@Nonnull FilterInternal filter, boolean negated) {
    this.filter = filter;
    this.negated = negated;
  }

  protected NegatableFilter(@Nonnull FilterInternal filter) {
    this(filter, false);
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return negated
        ? this.filter.applyOSHNegated(entity)
        : this.filter.applyOSH(entity);
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return negated
        ? this.filter.applyOSMNegated(entity)
        : this.filter.applyOSM(entity);
  }

  @Override
  public boolean applyOSMGeometry(OSMEntity entity, Supplier<Geometry> geometrySupplier) {
    return negated
        ? this.filter.applyOSMGeometryNegated(entity, geometrySupplier)
        : this.filter.applyOSMGeometry(entity, geometrySupplier);
  }

  @Override
  public boolean applyOSMEntitySnapshot(OSMEntitySnapshot snapshot) {
    return this.filter.applyOSMEntitySnapshot(snapshot) ^ this.negated;
  }

  @Override
  public boolean applyOSMContribution(OSMContribution contribution) {
    return this.filter.applyOSMContribution(contribution) ^ this.negated;
  }

  @Override
  public NegatableFilter negate() {
    return new NegatableFilter(this.filter, !this.negated);
  }

  @Override
  public String toString() {
    return (this.negated ? "not " : "") + this.filter.toString();
  }

}