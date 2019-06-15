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
import org.heigit.bigspatialdata.oshdb.util.TableNames;

/**
 * OSHDB database backend connector to a H2 database.
 */
public class OSHDBH2 extends OSHDBJdbc {

  /**
   * Opens a connection to oshdb data stored in a H2 database file.
   *
   * @param databaseFile the file name and path to the H2 database file. (the ".mv.db" file ending
   *        of H2 should be omitted here)
   * @throws SQLException if the database couldn't be opened
   * @throws ClassNotFoundException if the H2 database driver is not installed on the system
   */
  public OSHDBH2(String databaseFile) throws SQLException, ClassNotFoundException {
    super(
        "org.h2.Driver",
        "jdbc:h2:" + databaseFile.replaceAll("\\.mv\\.db$", "") + ";ACCESS_MODE_DATA=r"
    );
  }
  
  public OSHDBH2(Connection conn) throws ClassNotFoundException, SQLException {
    super(conn);
  }

  @Override
  public OSHDBH2 prefix(String prefix) {
    return (OSHDBH2) super.prefix(prefix);
  }

  @Override
  public OSHDBH2 multithreading(boolean useMultithreading) {
    return (OSHDBH2) super.multithreading(useMultithreading);
  }

  /**
   * Creates an in-memory copy of the current oshdb data (using a volatile in-memory H2 database),
   * for faster subsequent queries.
   *
   * <p>The original database connection will be closed during this process.</p>
   *
   * <p>Note that once the data has been cached in memory, this cannot be undone anymore by calling
   * this method like `.inMemory(false)`.</p>
   *
   * @param cache wether in-memory caching should be activated or not
   * @return an OSHDBDatabase using the cached in-memory copy of the oshdb data
   * @throws SQLException if there's a problem while copying the data into memory
   */
  public OSHDBH2 inMemory(boolean cache) throws SQLException {
    if (!cache) {
      return this;
    }

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
          ResultSet rs = srcStmt.executeQuery("show columns from " + tablename);
          List<String> columnNames = new LinkedList<>();
          while (rs.next()) {
            columnNames.add(rs.getString(1));
          }
          String columns = columnNames.stream().collect(Collectors.joining(", "));
          String placeholders = columnNames.stream()
              .map(ignored -> "?")
              .collect(Collectors.joining(", "));

          PreparedStatement destStmt = dest.prepareStatement(
              "insert into " + tablename + "(" + columns + ") values (" + placeholders + ")"
          );
          rs = srcStmt.executeQuery("select " + columns + " from " + tablename);
          while (rs.next()) {
            for (int i = 1; i <= columnNames.size(); i++) {
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


    this.connection = dest;
    return this;
  }

}
