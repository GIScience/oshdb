package org.heigit.bigspatialdata.hosmdb.etl.transform;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.heigit.bigspatialdata.hosmdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfRelation;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfTag;

public class TransformMapper2 {

  protected static final int NODE = 0;
  protected static final int WAY = 1;
  protected static final int RELATION = 2;

  private PreparedStatement pstmtKeyValue = null;
  private PreparedStatement pstmtRole = null;

  protected Map<String, Map<String, int[]>> keyValueCache = new HashMap<>();


  protected void initKeyTables(Connection connKeyTables) throws SQLException {
    pstmtKeyValue = connKeyTables.prepareStatement("" + //
        "select keyid, valueid " + //
        "from key join keyvalue on key.id = keyvalue.keyid " + //
        "where key.txt = ? and keyvalue.txt = ? ");
    pstmtRole = connKeyTables.prepareStatement("" + //
        "select id from role where txt = ?");
  }

  protected void clearKeyValueCache() {
    keyValueCache.clear();
  }

  protected int[] getKeyValue(List<OSMPbfTag> tags) {
    int[] keyValues = new int[tags.size() * 2];
    int i = 0;
    int key;
    int value;

    for (OSMPbfTag tag : tags) {
      try {
        Map<String, int[]> keyCache = keyValueCache.get(tag.getKey());
        if (keyCache == null) {
          keyCache = new HashMap<>();
          keyValueCache.put(tag.getKey(), keyCache);
        }
        int[] keyValue = keyCache.get(tag.getValue());
        if (keyValue != null) {
          keyValues[i++] = keyValue[0];
          keyValues[i++] = keyValue[1];
          continue;
        }


        pstmtKeyValue.setString(1, tag.getKey());
        pstmtKeyValue.setString(2, tag.getValue());

        try (ResultSet row = pstmtKeyValue.executeQuery()) {
          if (row.next()) {
            key = row.getInt(1);
            value = row.getInt(2);

            keyValues[i++] = key;
            keyValues[i++] = value;
            keyCache.put(tag.getValue(), new int[] {key, value});
          } else {
            keyValues[i++] = Integer.MAX_VALUE;
            keyValues[i++] = Integer.MAX_VALUE;
          }


        }
      } catch (SQLException e) {
        e.printStackTrace();
        keyValues[i++] = Integer.MAX_VALUE;
        keyValues[i++] = Integer.MAX_VALUE;
      }
    }
    return keyValues;
  }

  protected int getRole(String role) {
    try {
      pstmtRole.setString(1, role);
      try (ResultSet row = pstmtRole.executeQuery()) {
        if (row.next()) {
          return row.getInt(1);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return Integer.MAX_VALUE;
  }

  protected OSMMember[] convertLongs(List<Long> refs) {
    OSMMember[] ret = new OSMMember[refs.size()];
    int i = 0;
    for (Long ref : refs) {
      ret[i++] = new OSMMember(ref.longValue(),0,-1);
    }
    return ret;
  }

  protected OSMMember[] convertMembers(List<OSMPbfRelation.OSMMember> members) {
    OSMMember[] ret = new OSMMember[members.size()];
    for (int i = 0; i < members.size(); i++) {
      ret[i] = new OSMMember(members.get(i).getMemId(),members.get(i).getType(),
          getRole(members.get(i).getRole()));
    }
    return ret;
  }

  protected Object[] getRelation(PreparedStatement pstmt, long id) {
    try {
      pstmt.setLong(1, id);
    } catch (SQLException e) {
      e.printStackTrace();
      return new Long[0];
    }
    try (ResultSet rs = pstmt.executeQuery()) {
      while (rs.next()) {
        Object[] array = (Object[]) rs.getArray(1).getArray();
        return array;
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return new Long[0];
  }

}
