package org.heigit.ohsome.filter;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.jetbrains.annotations.Contract;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;

/**
 * A filter which selects OSM features by their geometry type (i.e., point, line or polygon).
 */
public class GeometryTypeFilter implements Filter {

  public enum GeometryType {
    POINT,
    LINE,
    POLYGON,
    OTHER
  }

  private static final Set<OSMType> POLYGON_TYPES = EnumSet.of(OSMType.WAY, OSMType.RELATION);
  private static final OSMTag tagTypeMultipolygon = new OSMTag("type", "multipolygon");
  private static final OSMTag tagTypeBoundary = new OSMTag("type", "boundary");
  private final OSHDBTag typeMultipolygon;
  private final OSHDBTag typeBoundary;

  private final GeometryType geometryType;
  private final TagTranslator tagTranslator;

  /**
   * Returns a new geometry type filter object.
   *
   * @param geometryType The type of tag filter, such as "point", "line", "polygon" or "other".
   * @param tt Tag Translator object for converting OSM tags to OSHDB tag ids
   */
  public GeometryTypeFilter(@Nonnull GeometryType geometryType, TagTranslator tt) {
    this.typeMultipolygon = tt.getOSHDBTagOf(tagTypeMultipolygon);
    this.typeBoundary = tt.getOSHDBTagOf(tagTypeBoundary);
    this.tagTranslator = tt;
    this.geometryType = geometryType;
  }

  /**
   * Returns the specified geometry type of this filter.
   *
   * @return the geometry type of this filter (POINT, LINE, POLYGON or OTHER).
   */
  @Contract(pure = true)
  public GeometryType getGeometryType() {
    return this.geometryType;
  }

  /**
   * Returns associated osm types of the geometry type filter.
   *
   * <p>For example, if the geometry type filter is set to POINT (`geometry:point`), this will
   * return a set containing OSMType.NODE. Or if the filter is for POLYGONs (`geometry:polygon`),
   * a set containing both OSMType.WAY and OSMType.RELATION will be returned.</p>
   *
   * @return the OSM types associated with the geometry type filter.
   */
  @Contract(pure = true)
  public Set<OSMType> getOSMTypes() {
    switch (geometryType) {
      case POINT:
        return EnumSet.of(OSMType.NODE);
      case LINE:
        return EnumSet.of(OSMType.WAY);
      case POLYGON:
        return POLYGON_TYPES;
      case OTHER:
      default:
        return EnumSet.of(OSMType.RELATION);
    }
  }

  @Contract(pure = true)
  private boolean checkOSMType(OSMType osmType) {
    switch (geometryType) {
      case POINT:
        return osmType == OSMType.NODE;
      case LINE:
        return osmType == OSMType.WAY;
      case POLYGON:
        return POLYGON_TYPES.contains(osmType);
      case OTHER:
      default:
        return osmType == OSMType.RELATION;
    }
  }

  @Contract(pure = true)
  private boolean checkGeometryType(Geometry geometry) {
    switch (geometryType) {
      case POINT:
        return geometry instanceof Puntal;
      case LINE:
        return geometry instanceof Lineal;
      case POLYGON:
        return geometry instanceof Polygonal;
      case OTHER:
      default:
        return geometry instanceof GeometryCollection
            && !(geometry instanceof Puntal
            || geometry instanceof Lineal
            || geometry instanceof Polygonal);
    }
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return checkOSMType(entity.getType());
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    final OSMType osmType = entity.getType();
    if (!checkOSMType(osmType)) {
      return false;
    }
    // type specific checks
    if (geometryType == GeometryType.POLYGON) {
      if (osmType == OSMType.WAY) {
        OSMMember[] wayNodes = ((OSMWay) entity).getRefs();
        return wayNodes.length >= 4 && wayNodes[0].getId() == wayNodes[wayNodes.length - 1].getId();
      } else if (osmType == OSMType.RELATION) {
        return entity.hasTagValue(typeMultipolygon.getKey(), typeMultipolygon.getValue())
            || entity.hasTagValue(typeBoundary.getKey(), typeBoundary.getValue());
      }
    }
    return true;
  }

  @Override
  public boolean applyOSMGeometry(OSMEntity entity, Geometry geometry) {
    return checkOSMType(entity.getType())
        && checkGeometryType(geometry);
  }

  @Override
  public FilterExpression negate() {
    EnumSet<GeometryType> otherTypes = EnumSet.allOf(GeometryType.class);
    otherTypes.remove(this.geometryType);

    List<GeometryType> otherTypesList = new ArrayList<>(otherTypes);
    assert otherTypesList.size() == 3 :
        "the negation of one geometry type must equal exactly tree geometry types";

    return new OrOperator(
        new GeometryTypeFilter(otherTypesList.get(0), this.tagTranslator),
        new OrOperator(
            new GeometryTypeFilter(otherTypesList.get(1), this.tagTranslator),
            new GeometryTypeFilter(otherTypesList.get(2), this.tagTranslator)
        )
    );
  }

  @Override
  public String toString() {
    return "geometry:" + geometryType.toString().toLowerCase();
  }
}
