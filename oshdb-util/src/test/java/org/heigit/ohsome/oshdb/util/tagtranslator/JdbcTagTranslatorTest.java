package org.heigit.ohsome.oshdb.util.tagtranslator;

/**
 * Tests the {@link JdbcTagTranslator} class.
 */
class JdbcTagTranslatorTest extends AbstractTagTranslatorTest {

  @Override
  TagTranslator getTranslator() {
    return new JdbcTagTranslator(source);
  }

}
