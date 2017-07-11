package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.Arrays;
import java.util.List;

import org.geotools.data.simple.SimpleFeatureSource;
import org.heigit.bigspatialdata.oshdb.examples.osmatrix.OSMatrixProcessor.TABLE;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

public class TotalNumbOfPOIs extends Attribute {

	@Override
	public double defaultValue() {
		return 0.0;
	}

	@Override
	public String getName() {
		return "totalNumbOfPOIs";
	}

	@Override
	public String getDescription() {
		return "The number of points of interest within the given cell.";
	}

	@Override
	public List<TABLE> getDependencies() {
		return Arrays.asList(TABLE.NODE, TABLE.WAY, TABLE.RELATION);
	}

//	@Override
//	protected String where(TABLE table) {
//		return "amenity like '%transport%' or amenity like '%shop%' or amenity like '%tourism%' or " +
//				"amenity like '%leisure%' or amenity like '%sport%'"; 
//	}

	@Override
	protected boolean needArea(TABLE table) {
		return false;
	}

	@Override
	public String getTitle() {
		return "Number of POIs";
	}

  @Override
  public AttributeCells compute(SimpleFeatureSource cellsIndex, OSHEntity<OSMEntity> osh, TagLookup tagLookup,
      List<Long> timestampsList, int attributeId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void aggregate(AttributeCells gridcellOutput, AttributeCells oshresult, List<Long> timestampList) {
    // TODO Auto-generated method stub
    
  }


}
