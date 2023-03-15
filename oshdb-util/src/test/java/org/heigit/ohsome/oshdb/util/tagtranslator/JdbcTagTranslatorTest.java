package org.heigit.ohsome.oshdb.util.tagtranslator;

import static java.lang.String.format;
import static org.heigit.ohsome.oshdb.util.TableNames.E_KEY;
import static org.heigit.ohsome.oshdb.util.TableNames.E_KEYVALUE;
import static org.heigit.ohsome.oshdb.util.TableNames.E_ROLE;
import static org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator.TranslationOption.ADD_MISSING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.Set;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcConnectionPool;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.util.KeyTables;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link JdbcTagTranslator} class.
 */
class JdbcTagTranslatorTest extends AbstractTagTranslatorTest {

  private static final OSMTag TAG_HIGHWAY_RESIDENTIAL = new OSMTag("highway", "residential");
  private static final OSMTag TAG_HIGHWAY_PRIMARY = new OSMTag("highway", "primary");
  private static final OSMTag TAG_HIGHWAY_SECONDARY = new OSMTag("highway", "secondary");
  private static final OSMTag TAG_BUILDING_YES = new OSMTag("building", "yes");
  private static final OSMTag TAG_BUILDING_HUT = new OSMTag("building", "hut");

  private static final OSMRole ROLE_OUTER = new OSMRole("outer");
  private static final OSMRole ROLE_INNER = new OSMRole("inner");

  @Override
  TagTranslator getTranslator() {
    return new JdbcTagTranslator(source);
  }

  @Test
  void testAddMissingTagsRoles() throws SQLException {
    var dataSource = JdbcConnectionPool.create("jdbc:h2:mem:", "sa", "");
    try (var conn = dataSource.getConnection()) {
      KeyTables.init(conn);
    }

    var tagTranslator = new CachedTagTranslator(new JdbcTagTranslator(dataSource, false),1024);
    var tags = tagTranslator.getOSHDBTagOf(Set.of(TAG_HIGHWAY_RESIDENTIAL));
    assertTrue(tags.isEmpty());
    tags = tagTranslator.getOSHDBTagOf(Set.of(TAG_HIGHWAY_RESIDENTIAL), ADD_MISSING);
    assertFalse(tags.isEmpty());
    assertEquals(new OSHDBTag(0, 0), tags.get(TAG_HIGHWAY_RESIDENTIAL));
    tags = tagTranslator.getOSHDBTagOf(Set.of(
        TAG_HIGHWAY_PRIMARY,
        TAG_BUILDING_YES), ADD_MISSING);
    assertFalse(tags.isEmpty());
    assertEquals(new OSHDBTag(0,1), tags.get(TAG_HIGHWAY_PRIMARY));
    assertEquals(new OSHDBTag(1,0), tags.get(TAG_BUILDING_YES));

    tags = tagTranslator.getOSHDBTagOf(Set.of(
       TAG_HIGHWAY_SECONDARY,
       TAG_HIGHWAY_PRIMARY,
       TAG_BUILDING_HUT,
       TAG_BUILDING_YES), ADD_MISSING);

    assertEquals(4, tags.size());
    assertEquals(new OSHDBTag(1,1), tags.get(TAG_BUILDING_HUT));

    var roles = tagTranslator.getOSHDBRoleOf(Set.of(ROLE_OUTER));
    assertTrue(roles.isEmpty());
    roles = tagTranslator.getOSHDBRoleOf(Set.of(ROLE_OUTER), ADD_MISSING);
    assertFalse(roles.isEmpty());
    assertEquals(OSHDBRole.of(0), roles.get(ROLE_OUTER));
    roles = tagTranslator.getOSHDBRoleOf(Set.of(ROLE_OUTER, ROLE_INNER), ADD_MISSING);
    assertFalse(roles.isEmpty());
    assertEquals(2, roles.size());
    assertEquals(OSHDBRole.of(0), roles.get(ROLE_OUTER));
    assertEquals(OSHDBRole.of(1), roles.get(ROLE_INNER));
  }

  private void createTables(DataSource ds) throws SQLException {
    try (var conn = ds.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(format("create table if not exists %s (id int primary key, values int, txt varchar)", E_KEY));
      stmt.execute(format("create table if not exists %s (keyId int, valueId int, txt varchar, primary key (keyId,valueId))", E_KEYVALUE));
      stmt.execute(format("create table if not exists %s (id int primary key, txt varchar)", E_ROLE));
    }
  }
}
