package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.geotools.data.simple.SimpleFeatureSource;
import org.heigit.bigspatialdata.oshdb.examples.osmatrix.OSMatrixProcessor.TABLE;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;


public class TotalNumberOfUsers extends Attribute {

	Map<Long, Set<String>> cellUsers = new HashMap<Long, Set<String>>();
	
	Set<String> users = null;

	@Override
	public String getName() {
		return "totalNumberOfUsersInACell";
	}

	@Override
	public String getDescription() {
		return "The number of users that have edited at least one object within the given cell.";
	}

	@Override
	public double defaultValue() {
		return 0.0;
	}

	@Override
	public List<TABLE> getDependencies() {
		return Arrays.asList(TABLE.NODE, TABLE.WAY, TABLE.RELATION);
	}

	@Override
	public String getTitle() {
		return "Number of users";
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