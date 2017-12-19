package org.heigit.bigspatialdata.oshdb.grid;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.TagTranslator;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public abstract class GridOSHEntity<HOSM extends OSHEntity> implements Iterable<HOSM>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(GridOSHEntity.class);

  private static final long serialVersionUID = 1L;
  protected final long id;
  protected final int level;

  protected final long baseTimestamp;

  protected final long baseLongitude;
  protected final long baseLatitude;

  protected final long baseId;

  protected final int[] index;
  protected final byte[] data;

  public GridOSHEntity(final long id,
          final int level,
          final long baseId,
          final long baseTimestamp, final long baseLongitude, final long baseLatitude, final int[] index, final byte[] data) {

    this.id = id;
    this.level = level;
    this.baseTimestamp = baseTimestamp;
    this.baseLongitude = baseLongitude;
    this.baseLatitude = baseLatitude;
    this.baseId = baseId;

    this.index = index;
    this.data = data;
  }

  public long getId() {
    return id;
  }

  public int getLevel() {
    return level;
  }

  @Override
  public String toString() {
    try {
      BoundingBox bbox = XYGrid.getBoundingBox(new CellId((int) id, level));
      return String.format(Locale.ENGLISH, "ID:%d Level:%d BBox:(%f,%f),(%f,%f)", id, level, bbox.getMinLat(), bbox.getMinLon(), bbox.getMaxLat(), bbox.getMaxLon());
    } catch (CellId.cellIdExeption ex) {
      LOG.warn("", ex);
      return String.format(Locale.ENGLISH, "ID:%d Level:%d", id, level);
    }
  }

  /**
   * Get a GIS-compatible String version of your OSH-GridCell. Note that this
   * method uses only the latest version of the each underlying OSM-Entity to
   * construct geometries. If you desire inter-version-geometries, which are
   * possible, or specific geometries of all versions in this cell, please refer
   * to
   * {@link org.heigit.bigspatialdata.oshdb.osm.OSMEntity#toGeoJSON(java.util.List, org.heigit.bigspatialdata.oshdb.util.TagTranslator, org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter) the static method toGeoJSON of the OSMEntity}
   * and use it as desired. I hope you know what you are doing, this can get
   * pretty big and nasty!!!
   *
   * @param tagtranslator a connection to a database to translate the coded
   * integer back to human readable string
   * @param areaDecider A list of tags, that define a polygon from a linestring.
   * A default one is available.
   * @return A string representation of the Object in GeoJSON-format
   * (https://tools.ietf.org/html/rfc7946#section-3.3)
   */
  public String toGeoJSON(TagTranslator tagtranslator, TagInterpreter areaDecider) {
    List<Pair<? extends OSMEntity, Long>> entities = new ArrayList<>();

    Iterator<? extends OSHEntity> it = iterator();
    while (it.hasNext()) {
      OSHEntity obj = it.next();
      entities.add(new ImmutablePair<>(obj.getLatest(), obj.getLatest().getTimestamp()));
    }
    return OSMEntity.toGeoJSON(entities, tagtranslator, areaDecider);
  }
}
