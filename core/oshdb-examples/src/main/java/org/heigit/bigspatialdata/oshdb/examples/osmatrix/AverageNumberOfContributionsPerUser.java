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

public class AverageNumberOfContributionsPerUser extends Attribute {

	Map<Long, Set<String>> cellUsers = new HashMap<Long, Set<String>>();
	
	Map<Long, Long> cellNumContributions = new HashMap<Long, Long>();

	@Override
	public String getName() {
		return "averageNumberOfContributionsPerUser";
	}

	@Override
	public String getDescription() {
		return "The average number of objects that have been modified by a single user.";
	}

	@Override
	public List<TABLE> getDependencies() {
		 return Arrays.asList(TABLE.NODE,TABLE.WAY,TABLE.RELATION);
	}

	@Override
	protected void beforSend() {

		}

	@Override
	protected boolean needArea(TABLE table) {
		return false;
	}

	@Override
	public String getTitle() {
		return "Average number of contributions per user";
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