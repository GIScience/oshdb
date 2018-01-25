package org.heigit.bigspatialdata.oshdb.util.geometry;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Class uses the NaturalEarth 1:10 Cultural-Vectors to extract BoundingBoxes and Polygons. It
 * is meant to be a helper for
 * {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer#areaOfInterest(com.vividsolutions.jts.geom.Geometry&com.vividsolutions.jts.geom.Polygonal)
 * areaOfInterest}. Please note, only continental territories are supported, for now. Its underlying
 * source is form
 * <a href="http://www.naturalearthdata.com/downloads/10m-cultural-vectors/">here</a>. Please
 * consider the licence when going public with the project.
 *
 */
public class Country {

  private static final Logger LOG = LoggerFactory.getLogger(Country.class);

  /**
   * This is for advanced users who know how to use geotools. You may get your feature(s) from this
   * FeatureSource.
   *
   * @return A FeatureSource for the dataset.
   * @throws IOException
   */
  public static FeatureSource<SimpleFeatureType, SimpleFeature> getFeatureSource()
      throws IOException {
    Map<String, Object> map = new HashMap<>(1);
    map.put("url",
        Country.class.getResource("/ne_10m_admin_0_map_units/ne_10m_admin_0_map_units.shp"));

    DataStore dataStore = DataStoreFinder.getDataStore(map);
    String typeName = dataStore.getTypeNames()[0];
    return dataStore.getFeatureSource(typeName);
  }

  /**
   * Gets the exact geometry of a country. The output varies by the type of input you provide.
   * Please note that the data is not perfect. Results may be unwanted, please check the source.
   *
   * @param type
   * @param name
   * @return A MultiPolygon for simplicity, no matter what shape the country has.
   * @throws IOException
   */
  public static MultiPolygon getGeometry(CountryCodeType type, String name) throws IOException {
    if (type == CountryCodeType.ISO_A2) {
      if (name.length() > 2) {
        LOG.error("ISO_A2 name has more than 2 characters.");
        return null;
      }
    }
    return Country.getFeatures(type, name);

  }

  /**
   * Works the same as
   * {@link #getGeometry(org.heigit.bigspatialdata.oshdb.api.utils.CountryCodeType, java.lang.String)
   * getGeometry} but returns the bounding box. To keep you from creating large bounding boxes this
   * function is limited to {@link org.heigit.bigspatialdata.oshdb.api.utils.CountryCodeType#GEOUNIT
   * GEOUNIT}.
   *
   * @param name the {@link org.heigit.bigspatialdata.oshdb.api.utils.CountryCodeType#GEOUNIT
   *        GEOUNIT} name of the country
   * @return
   * @throws IOException
   */
  public static OSHDBBoundingBox getBoundingBox(String name) throws IOException {
    MultiPolygon mp = Country.getFeatures(CountryCodeType.GEOUNIT, name);
    if (mp == null) {
      return null;
    }
    Envelope env = mp.getEnvelopeInternal();
    return new OSHDBBoundingBox(env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY());

  }

  private static MultiPolygon getFeatures(CountryCodeType type, String name) throws IOException {
    FeatureSource<SimpleFeatureType, SimpleFeature> source = Country.getFeatureSource();
    FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures();
    GeometryFactory gf = new GeometryFactory();

    try (FeatureIterator<SimpleFeature> features = collection.features()) {
      Collection<Geometry> poligonArr = new ArrayList<>(1);
      while (features.hasNext()) {
        SimpleFeature feature = features.next();
        if (feature.getAttribute(type.toString()).equals(name)) {
          Geometry geom = (Geometry) feature.getDefaultGeometry();
          poligonArr.add(geom);
        }
      }
      GeometryCollection geometryCollection = (GeometryCollection) gf.buildGeometry(poligonArr);
      if (geometryCollection.isEmpty()) {
        LOG.warn("No feature was found");
        return null;
      }

      if (geometryCollection.getNumGeometries() > 1) {
        return (MultiPolygon) geometryCollection.union();
      } else {
        return (MultiPolygon) geometryCollection.getGeometryN(0);
      }

    }
  }

  private Country() {}

}
