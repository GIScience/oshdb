package org.heigit.ohsome.oshdb.source.osc;

import java.io.BufferedInputStream;
import org.heigit.ohsome.oshdb.util.tagtranslator.MemoryTagTranslator;
import org.junit.jupiter.api.Test;

class OscParserTest {

  @Test
  void entities() throws Exception {
    var tagTranslator = new MemoryTagTranslator();
//    try ( var osc = new BufferedInputStream(null);
//          var osmSource = new OscParser(osc)) {
//      var entities = osmSource.entities(tagTranslator);
//    }
  }
}