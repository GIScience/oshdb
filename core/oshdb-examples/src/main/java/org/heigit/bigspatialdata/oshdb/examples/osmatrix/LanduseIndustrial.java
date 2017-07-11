package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.functors.InstanceofPredicate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.heigit.bigspatialdata.oshdb.examples.osmatrix.OSMatrixProcessor.TABLE;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.Geo;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.TopologyException;
import com.vividsolutions.jts.io.WKTWriter;

public class LanduseIndustrial extends Attribute{

	private static final Logger logger = Logger.getLogger(LanduseIndustrial.class);
	
	Set<String> values = new HashSet<String>();

	@Override
	public String getName() {
		return "landuse_industrial";
	}

	@Override
	public String getDescription() {
		return "The area covered by industrial zones given in square meters.";
	}

	@Override
	public List<TABLE> getDependencies() {
		return Arrays.asList(TABLE.WAY, TABLE.RELATION);
	}

	@Override
	protected boolean needArea(TABLE table) {
		return true;
	}

	@Override
	public String getTitle() {
		return "Area of industrial zones";
	}

  @Override
  public AttributeCells compute(SimpleFeatureSource cellsIndex, OSHEntity<OSMEntity> osh, TagLookup tagLookup, List<Long> timestampsList, int attributeId) {
    AttributeCells oshresult = new AttributeCells();
    
    Pair tagKeyValue = tagLookup.getAllKeyValues().get("landuse").get("industrial");
    int tagKey = (int) tagKeyValue.getLeft();
    int tagValue = (int) tagKeyValue.getRight();
    
    //filter all osh that have never been an amenity
    if ( !(osh.hasTagKey(tagKey) && ( osh.getType()== OSHEntity.WAY || osh.getType()==OSHEntity.RELATION))) return oshresult; //empty
     
    final FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
    
    
    for ( Map.Entry<Long,OSMEntity> entry : osh.getByTimestamps(timestampsList).entrySet() ){
      
      long ts = entry.getKey();
      OSMEntity osm = entry.getValue();
      
      if(! osm.hasTagKey(tagKey) || !osm.isVisible() ) return oshresult;
     
      Geometry osmGeometry;
      try{
      osmGeometry= osm.getGeometry(ts, tagLookup.getTagInterpreter());
      
      

      
      
      } catch (Exception e) {
        osmGeometry = null;
      }
      
      if(osmGeometry == null)
        continue;
      
      final Filter filter = ff.intersects(ff.property(cellsIndex.getSchema().getGeometryDescriptor().getLocalName()),
          ff.literal(osmGeometry));
      
      try {
        FeatureCollection<SimpleFeatureType, SimpleFeature> osmatrixCells = cellsIndex.getFeatures(filter) ;
      
        FeatureIterator<SimpleFeature> features = osmatrixCells.features();
        
        while (features.hasNext()) {
          
          SimpleFeature osmatrixZelle = features.next();

          double area = 0.0;
          
          try{
            
            //System.out.println(((Geometry)osmatrixZelle.getDefaultGeometry()).getSRID());
              
              Geometry intersected = osmGeometry.intersection((Geometry) osmatrixZelle.getDefaultGeometry());
              if (intersected instanceof Polygon) {
                Polygon poly = (Polygon) intersected;
                area = Geo.areaOf(poly);
                
                Double cellId_ = Double.parseDouble(osmatrixZelle.getAttribute("id").toString() );
                long cellId = cellId_.longValue();
                if(cellId==172019){
                   CsvLogger.logCsv(intersected, cellId, ts, "test");
                }
              }
              else if (intersected instanceof MultiPolygon){
                MultiPolygon multi = (MultiPolygon) intersected;
                area = Geo.areaOf(multi);
                
                Double cellId_ = Double.parseDouble(osmatrixZelle.getAttribute("id").toString() );
                long cellId = cellId_.longValue();
                if(cellId==172019){
                CsvLogger.logCsv(intersected, cellId, ts, "test");
                }
              }
              
          }
          catch (TopologyException e) {
          //  logger.debug("Topology Exeption.");
            //e.printStackTrace();
          }
          Double cellId_ = Double.parseDouble(osmatrixZelle.getAttribute("id").toString() );
          long cellId = cellId_.longValue();
//          CsvLogger.logCsv(intersected, cellId, ts, "test");
//          System.out.println("attributeId " + attributeId);
            oshresult.get(attributeId).get(cellId).get(ts).setValueWeight(area, 1);
            
          
        }

        

      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
    }  

  
    return oshresult;
  }

  @Override
  public void aggregate(AttributeCells gridcellOutput, AttributeCells oshresult, List<Long> timestampList) {
    for (Map.Entry<Integer, CellTimeStamps>  attributeCell : oshresult.map.entrySet()){
      
      final CellTimeStamps cellTimestamps = attributeCell.getValue();
      
      for ( Map.Entry<Long, TimeStampValuesWeights> cellTimestamp : cellTimestamps.map.entrySet()){
        
        final TimeStampValuesWeights timestampValueWeights = cellTimestamp.getValue();
        
        for (Long long1 : timestampList) {
          timestampValueWeights.map.putIfAbsent(long1, new ValueWeight());
        } 
            
        
        for ( Map.Entry<Long, ValueWeight> timestampValueWeight : timestampValueWeights.map.entrySet() ){
          
          final ValueWeight valueWeight = timestampValueWeight.getValue();
          //System.out.println("valueWeight: " + valueWeight.getValue());
          
          ValueWeight partial = gridcellOutput.get(attributeCell.getKey()).get(cellTimestamp.getKey()).get(timestampValueWeight.getKey());
//          System.out.println("partial before: " + partial.getValue());
          //count
          partial.setValue(partial.getValue() + valueWeight.getValue() );
//          System.out.println("partial after: " + partial.getValue());
          
          gridcellOutput
          .get(attributeCell.getKey())
          .get(cellTimestamp.getKey())
          .get(timestampValueWeight.getKey())
          .setValue(partial.getValue());
          
        }
        
      }
      
    }
    
  }
    
  }

