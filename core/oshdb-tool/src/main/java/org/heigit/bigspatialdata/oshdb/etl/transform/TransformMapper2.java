package org.heigit.bigspatialdata.oshdb.etl.transform;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfRelation;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfTag;

public class TransformMapper2 {

  private PreparedStatement pstmtKeyValue = null;
  private PreparedStatement pstmtRole = null;

  protected Map<String, Map<String, int[]>> keyValueCache = new HashMap<>();

  protected void initKeyTables(Connection connKeyTables) throws SQLException {
    pstmtKeyValue = connKeyTables.prepareStatement(""
            + //
            "select keyid, valueid "
            + //
            "from " + TableNames.E_KEY.toString() + " join " + TableNames.E_KEYVALUE.toString() + " on " + TableNames.E_KEY.toString() + ".id = " + TableNames.E_KEYVALUE.toString() + ".keyid "
            + //
            "where " + TableNames.E_KEY.toString() + ".txt = ? and " + TableNames.E_KEYVALUE.toString() + ".txt = ? "
    );
    pstmtRole = connKeyTables.prepareStatement(""
            + //
            "select id from " + TableNames.E_ROLE.toString() + " where txt = ?");
  }

  protected void clearKeyValueCache() {
    keyValueCache.clear();
  }

  protected int[] getKeyValue(List<OSMPbfTag> tags) {
    int[] keyValue;

    List<int[]> sortedKeyValues = new ArrayList<>(tags.size());

    for (OSMPbfTag tag : tags) {
      try {
        Map<String, int[]> keyCache = keyValueCache.get(tag.getKey());
        if (keyCache == null) {
          keyCache = new HashMap<>();
          keyValueCache.put(tag.getKey(), keyCache);
        }
        keyValue = keyCache.get(tag.getValue());
        if (keyValue != null) {
          sortedKeyValues.add(keyValue);
          continue;
        }

        pstmtKeyValue.setString(1, tag.getKey());
        pstmtKeyValue.setString(2, tag.getValue());

        try (ResultSet row = pstmtKeyValue.executeQuery()) {
          if (row.next()) {
            keyValue = new int[]{row.getInt(1), row.getInt(2)};
            sortedKeyValues.add(keyValue);
            keyCache.put(tag.getValue(), keyValue);
          } else {
            // ignore if key/value is not in the keytable database
          }
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    Collections.sort(sortedKeyValues,
            (a, b) -> {
              int c = Integer.compare(a[0], b[0]);
              if (c == 0) {
                c = Integer.compare(a[1], b[1]);
              }
              return c;

            });

    int[] keyValues = new int[sortedKeyValues.size() * 2];
    int i = 0;
    for (int[] kv : sortedKeyValues) {
      keyValues[i++] = kv[0];
      keyValues[i++] = kv[1];
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
      ret[i++] = new OSMMember(ref.longValue(), OSMType.NODE, -1);
    }
    return ret;
  }

  protected OSMMember[] convertMembers(List<OSMPbfRelation.OSMMember> members) {
    OSMMember[] ret = new OSMMember[members.size()];
    for (int i = 0; i < members.size(); i++) {
        ret[i] = new OSMMember(members.get(i).getMemId(), OSMType.fromInt(members.get(i).getType()),
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
