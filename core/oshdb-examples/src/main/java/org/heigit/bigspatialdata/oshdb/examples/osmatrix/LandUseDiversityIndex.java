package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.heigit.bigspatialdata.oshdb.examples.osmatrix.OSMatrixProcessor.TABLE;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class LandUseDiversityIndex extends Attribute{

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return "LandUseDiversityIndex";
  }

  @Override
  public String getDescription() {
    // TODO Auto-generated method stub
    return "LandUseDiversityIndex";
  }

  @Override
  public String getTitle() {
    // TODO Auto-generated method stub
    return "LandUseDiversityIndex";
  }

  @Override
  public List<TABLE> getDependencies() {
    // TODO Auto-generated method stub
    return Arrays.asList(TABLE.WAY, TABLE.RELATION);
  }


  @Override
  public AttributeCells compute(SimpleFeatureSource cellsIndex, OSHEntity<OSMEntity> osh, TagLookup tagLookup,
    List<Long> timestampsList, int attributeId) {
   
    AttributeCells oshresult = new AttributeCells();
    
   
    Map<String,Pair <Integer, Integer >> landuseTags = tagLookup.getAllKeyValues().get("landuse");
    
    for (Map.Entry<String, Pair<Integer,Integer>> key : landuseTags.entrySet()) {
      
      
    }
   
    
    
    
    Pair tagKeyValue = tagLookup.getAllKeyValues().get("landuse").get("yes");
    int tagKey = (int) tagKeyValue.getLeft();
    int tagValue = (int) tagKeyValue.getRight();
    
    //filter all osh that have never been an amenity
    if ( !(osh.hasTagKey(tagKey) && ( osh.getType()== OSHEntity.WAY || osh.getType()==OSHEntity.RELATION))) return oshresult; //empty
    //System.out.println("Here");

    
    //System.out.println(tagKey);
    //System.out.println(cellsIndex.getSchema().getGeometryDescriptor().getLocalName());
    
    
    final FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
    
    
    for ( Map.Entry<Long,OSMEntity> entry : osh.getByTimestamps(timestampsList).entrySet() ){
      
      long ts = entry.getKey();
      OSMEntity osm = entry.getValue();
      
      if(! osm.hasTagKey(tagKey) || !osm.isVisible() ) return oshresult;
      
//      System.out.printf("%d  ->  %s\n",ts,osm);
      Geometry osmGeometry;
      try{
      osmGeometry= osm.getGeometry(ts, tagLookup.getTagInterpreter());
      } catch (Exception e) {
        osmGeometry = null;
      }
      if(osmGeometry == null)
        continue;
      Point osmCentroid = osmGeometry.getCentroid();  
      
      final Filter filter = ff.intersects(ff.property(cellsIndex.getSchema().getGeometryDescriptor().getLocalName()),
          ff.literal(osmCentroid));
      
      
      try {
        FeatureCollection<SimpleFeatureType, SimpleFeature> osmatrixCells = cellsIndex.getFeatures( filter) ;
      
        FeatureIterator<SimpleFeature> features = osmatrixCells.features();
        
        while (features.hasNext()) {
          
          SimpleFeature feature = features.next();
          
          Double cellId_ = Double.parseDouble( feature.getAttribute("id").toString() );
          long cellId = cellId_.longValue();
//          System.out.println("attributeId " + attributeId);
         oshresult.get(attributeId).get(cellId).get(ts).setValueWeight(1, 1);
         
          
        }
        
        //if (!osmatrixCells.isEmpty()) System.out.println(osmatrixCells.features().next().getAttributes());
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
      
      
     
      
    }
   
    // Iterator<OSMEntity> osmIterator
    return oshresult;
  }

  @Override
  public void aggregate(AttributeCells gridcellOutput, AttributeCells oshresult, List<Long> timestampsList) {
    for (Map.Entry<Integer, CellTimeStamps>  attributeCell : oshresult.map.entrySet()){
      
      final CellTimeStamps cellTimestamps = attributeCell.getValue();
      
      for ( Map.Entry<Long, TimeStampValuesWeights> cellTimestamp : cellTimestamps.map.entrySet()){
        
        final TimeStampValuesWeights timestampValueWeights = cellTimestamp.getValue();
        
        for (Long long1 : timestampsList) {
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
