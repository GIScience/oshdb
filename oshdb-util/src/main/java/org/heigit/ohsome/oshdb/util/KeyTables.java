package org.heigit.ohsome.oshdb.util;

import static java.lang.String.format;
import static org.heigit.ohsome.oshdb.util.TableNames.E_KEY;
import static org.heigit.ohsome.oshdb.util.TableNames.E_KEYVALUE;

import java.sql.Connection;
import java.sql.SQLException;

public class KeyTables {

  private KeyTables(){
    // utitily class
  }

  /**
   * Initial Keytables tables.
   * @param conn connection to keytables database
   * @throws SQLException
   */
  public static void init(Connection conn) throws SQLException {
    try (var stmt = conn.createStatement()) {
      stmt.execute("create table if not exists tag_key (id int primary key, txt varchar, values int)");
      stmt.execute("create table if not exists tag_value (keyid int, valueid int, txt varchar, primary key (keyId,valueId))");
      stmt.execute("create table if not exists role (id int primary key, txt varchar)");
      stmt.execute("create table if not exists metadata (key varchar primary key, value varchar)");

      // view for backward compatibility
      stmt.execute(format("create view %s as select id, txt, values from tag_key", E_KEY));
      stmt.execute(format("create view %s as select keyid, valueId, txt from tag_value", E_KEYVALUE));
    }
  }
}
