package org.heigit.ohsome.oshdb.source;

import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import reactor.core.publisher.Flux;

public interface OSMSource {

  Flux<OSMEntity> entities(TagTranslator tagTranslator);
}
