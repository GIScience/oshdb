package org.heigit.bigspatialdata.hosmdb.grid;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.hosmdb.osh.HOSMEntity;
import org.heigit.bigspatialdata.hosmdb.osh.HOSMNode;
import org.heigit.bigspatialdata.hosmdb.osh.HOSMRelation;
import org.heigit.bigspatialdata.hosmdb.osh.HOSMWay;
import org.heigit.bigspatialdata.hosmdb.osm.OSMMember;
import org.heigit.bigspatialdata.hosmdb.osm.OSMNode;
import org.heigit.bigspatialdata.hosmdb.osm.OSMRelation;
import org.heigit.bigspatialdata.hosmdb.osm.OSMWay;
import org.junit.Test;

public class TestHOSMCellRelation {

  @Test
  public void test() throws IOException {
    HOSMNode node100 = buildHOSMNode(
        Arrays.asList(new OSMNode(100l, 1, 1l, 0l, 123, new int[] {1, 2}, 494094984l, 86809727l)));
    HOSMNode node102 = buildHOSMNode(
        Arrays.asList(new OSMNode(102l, 1, 1l, 0l, 123, new int[] {2, 1}, 494094984l, 86809727l)));
    HOSMNode node104 = buildHOSMNode(
        Arrays.asList(new OSMNode(104l, 1, 1l, 0l, 123, new int[] {2, 4}, 494094984l, 86809727l)));
    
    HOSMWay way200 = buildHOSMWay(Arrays.asList(new OSMWay(200, 1, 3333l, 4444l, 23, new int[] {1, 2}, new OSMMember[] {new OSMMember(100, 0, 0),new OSMMember(104,0,0)})),Arrays.asList(node100,node104));
    HOSMWay way202 = buildHOSMWay(Arrays.asList(new OSMWay(202, 1, 3333l, 4444l, 23, new int[] {1, 2}, new OSMMember[] {new OSMMember(100, 0, 0),new OSMMember(102,0,0)})),Arrays.asList(node100,node102)); 
    



    HOSMRelation relation300 = HOSMRelation.build(Arrays.asList(//
        new OSMRelation(300, 1, 3333l, 4444l, 23, new int[] {},     new OSMMember[] {new OSMMember(100, -1, 0, null), new OSMMember(102, -1, 0, null)}), //
        new OSMRelation(300, 2, 3333l, 4444l, 23, new int[] {1, 2}, new OSMMember[] {new OSMMember(100, -1, 0, null), new OSMMember(102, -1, 0, null)})), //
        Arrays.asList(node100, node102), Arrays.asList());
    
    HOSMRelation relation301 = HOSMRelation.build(Arrays.asList(//
        new OSMRelation(301, 1, 3333l, 4444l, 23, new int[] {},     new OSMMember[] {new OSMMember(200, -1, 1, null), new OSMMember(202, -1, 1, null)}), //
        new OSMRelation(301, 2, 3333l, 4444l, 23, new int[] {1, 2}, new OSMMember[] {new OSMMember(200, -1, 1, null), new OSMMember(202, -1, 1, null)})), //
        Arrays.asList(), Arrays.asList(way200,way202));

    
    long cellId = 1;
    int  cellLevel = 2;
    long baseId = 1234;
    
    HOSMCellRelations hosmCell = HOSMCellRelations.compact(cellId, cellLevel, baseId, 0, 0, 0, Arrays.asList(relation300,relation301));

    hosmCell.forEach(osh -> {
      HOSMRelation oshRelation = (HOSMRelation)osh;
      try {
        System.out.printf("%d (%s) %d\n",oshRelation.getId(), print((List<HOSMEntity>)(List)oshRelation.getNodes()), oshRelation.getWays().size());
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
    });


  }

  
  private String print(List<HOSMEntity> entity){
    if(entity.isEmpty())
      return "";
    
    StringBuilder sb = new StringBuilder();
    for (HOSMEntity hosm : entity) {
      sb.append(hosm.getId());
      
      sb.append(',');
    }
    return sb.toString();
  }
  
  static HOSMNode buildHOSMNode(List<OSMNode> versions){
    try {
      return HOSMNode.build(versions);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
  
  static HOSMWay buildHOSMWay(List<OSMWay> versions, List<HOSMNode> nodes){
    try {
      return HOSMWay.build(versions,nodes);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
  
}
