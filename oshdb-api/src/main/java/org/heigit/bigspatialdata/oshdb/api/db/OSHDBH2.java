package org.heigit.bigspatialdata.oshdb.api.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.heigit.bigspatialdata.oshdb.TableNames;

public class OSHDBH2 extends OSHDBJdbc {

  public OSHDBH2(String databaseFile) throws SQLException, ClassNotFoundException {
    super("org.h2.Driver", "jdbc:h2:" + databaseFile.replaceAll("\\.mv\\.db$", "") + ";ACCESS_MODE_DATA=r");
  }
  
  public OSHDBH2(Connection conn) throws ClassNotFoundException, SQLException{
    super(conn);
  }

  public OSHDBH2 multithreading(boolean useMultithreading) {
    super.multithreading(useMultithreading);
    return this;
  }

  /**
   * Creates an in-memory copy of the current oshdb data (using a volatile in-memory H2 database),
   * for faster subsequent queries.
   *
   * The original database connection will be closed during this process.
   *
   * Note that once the data has been cached in memory, this cannot be undone anymore by calling
   * this method like `.inMemory(false)`.
   *
   * @param cache wether in-memory caching should be activated or not
   * @return an OSHDBDatabase using the cached in-memory copy of the oshdb data
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  public OSHDBH2 inMemory(boolean cache) throws ClassNotFoundException, SQLException {
    if(!cache) return this;

    Connection dest = DriverManager.getConnection("jdbc:h2:mem:");
    try (Connection src = this.getConnection()) {
      try (
          Statement srcStmt = src.createStatement();
          ResultSet srcRst = srcStmt.executeQuery("script nodata")
      ) {
        try (Statement destStmt = dest.createStatement()) {
          while (srcRst.next()) {
            destStmt.executeUpdate(srcRst.getString(1));
          }
        }
      }

      Consumer<String> copyData = tablename -> {
        try (Statement srcStmt = src.createStatement()) {
          ResultSet rs = srcStmt.executeQuery("show columns from "+tablename);
          List<String> columnNames = new LinkedList<>();
          while (rs.next()) {
            columnNames.add(rs.getString(1));
          }
          String columns = columnNames.stream().collect(Collectors.joining(", "));
          String placeholders = columnNames.stream().map(ignored -> "?").collect(Collectors.joining(", "));

          PreparedStatement destStmt = dest.prepareStatement("insert into "+tablename+"("+columns+") values ("+placeholders+")");
          rs = srcStmt.executeQuery("select "+columns+" from "+tablename);
          while (rs.next()) {
            for (int i=1; i<=columnNames.size(); i++) {
              destStmt.setObject(i, rs.getObject(i));
            }
            destStmt.execute();
          }
        } catch (SQLException e) {
          e.printStackTrace();
        }
      };

      try (Statement srcStmt = src.createStatement()) {
        ResultSet rs = srcStmt.executeQuery("show tables");
        while (rs.next()) {
          String tableName = rs.getString(1).toLowerCase();
          // we only need to cache tables that match the currently selected table prefix
          Set<String> tableNames = Stream.of(TableNames.values())
              .map(x -> x.toString(this.prefix()))
              .map(String::toLowerCase)
              .collect(Collectors.toSet());
          if (tableNames.contains(tableName)) {
            copyData.accept(tableName);
          }
        }
      }
    }


    this._conn = dest;
    return this;
  }

}
