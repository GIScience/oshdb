package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.geotools.data.simple.SimpleFeatureSource;
import org.heigit.bigspatialdata.oshdb.examples.osmatrix.OSMatrixProcessor.TABLE;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;


public class MaxNumberOfModifiedObjectsPerUser extends Attribute {

	Map<Long, Map<Integer, Long>> cellUserContributions = new HashMap<Long, Map<Integer, Long>>();

	@Override
	public String getName() {
		return "maxNumberOfModifiedObjectsPerUser";
	}

	@Override
	public String getDescription() {
		return "The maximum number of objects that have been modified by a single user.";
	}

	@Override
	public double defaultValue() {
		return Double.MIN_VALUE;
	}

	@Override
	public List<TABLE> getDependencies() {
		return Arrays.asList(TABLE.NODE, TABLE.WAY, TABLE.RELATION);
	}
	@Override
	protected void beforSend() {

	}

	@Override
	public String getTitle() {
		return "Maximum number of modified objects per user";
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