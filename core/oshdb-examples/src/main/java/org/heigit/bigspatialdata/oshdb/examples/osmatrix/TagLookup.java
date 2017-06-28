package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.json.simple.parser.ParseException;

public class TagLookup {
  
  Connection conn;

  public TagLookup(Connection conn) throws SQLException, IOException, ParseException {
   
    this.conn = conn;
    createAllRoles();
    createAllKeyValues();
    createTagInterpreter();
    
    
  }
  
  private Map<String,Integer> allRoles = new HashMap<String, Integer>(); //Collections.emptyMap();
  private Map<String,Map<String, Pair<Integer,Integer>>> allKeyValues = new HashMap<String, Map<String,Pair<Integer,Integer>>>(); //Collections.emptyMap();
  private TagInterpreter tagInterpreter;
  
  
  
  public Map<String, Integer> getAllRoles() {
    return allRoles;
  }

  public Map<String, Map<String, Pair<Integer, Integer>>> getAllKeyValues() {
    return allKeyValues;
  }

  public TagInterpreter getTagInterpreter() {
    return tagInterpreter;
  }

  private void createAllKeyValues() throws SQLException{    
  
   Statement stmt = conn.createStatement();
  ResultSet rstTags = stmt.executeQuery(
      "select k.ID as KEYID, kv.VALUEID as VALUEID, k.txt as KEY, kv.txt as VALUE from KEYVALUE kv inner join KEY k on k.ID = kv.KEYID;");
  //Map<String, Map<String, Pair<Integer, Integer>>> allKeyValues = new HashMap<>();
  while (rstTags.next()) {
    int keyId = rstTags.getInt(1);
    int valueId = rstTags.getInt(2);
    String keyStr = rstTags.getString(3);
    String valueStr = rstTags.getString(4);
    if (!allKeyValues.containsKey(keyStr))
      allKeyValues.put(keyStr, new HashMap<>());
    allKeyValues.get(keyStr).put(valueStr, new ImmutablePair<>(keyId, valueId));
  }
  rstTags.close();
  //return allKeyValues;
  }
  
  private void createAllRoles() throws SQLException{
   Statement stmt = conn.createStatement();
  ResultSet rstRoles = stmt.executeQuery("select ID as ROLEID, txt as ROLE from ROLE;");
  //Map<String, Integer> allRoles = new HashMap<>();
  while (rstRoles.next()) {
    int roleId = rstRoles.getInt(1);
    String roleStr = rstRoles.getString(2);
    allRoles.put(roleStr, roleId);
    
  }
  rstRoles.close();
 // return allRoles;
  }
  
  private void createTagInterpreter() throws IOException, ParseException{
    tagInterpreter = new DefaultTagInterpreter(allKeyValues, allRoles);
  //return tagInterpreter;
  }
  

}
