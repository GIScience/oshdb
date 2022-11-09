package org.heigit.ohsome.oshdb.util.tagtranslator;

/**
 * Tests the {@link CachedTagTranslator} class.
 */
class CachedTagTranslatorTest extends AbstractTagTranslatorTest {

  @Override
  TagTranslator getTranslator() {
    return new CachedTagTranslator(new JdbcTagTranslator(source), 1000, 1000);
  }

}
