package org.heigit.ohsome.oshdb.source.osc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.TreeMap;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.util.tagtranslator.CachedTagTranslator;
import org.heigit.ohsome.oshdb.util.tagtranslator.MemoryTagTranslator;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;

class OscParserTest {

  private static final String osc = """
      <?xml version='1.0' encoding='UTF-8'?>
      <osmChange version="0.6" generator="osmdbt-create-diff/0.6">
        <modify>
          <node id="267259573" version="3" timestamp="2023-03-02T14:36:51Z" uid="12551976" user="rms_ALG" changeset="133214654" lat="50.2156476" lon="11.9501611">
            <tag k="entrance" v="yes"/>
            <tag k="level" v="1"/>
            <tag k="name" v="Ausgang Richtung Am Lamitzsteig"/>
            <tag k="public_transport" v="entrance"/>
            <tag k="ref" v="HM"/>
          </node>
        </modify>
        <delete>
            <node id="419010513" version="3" timestamp="2023-03-02T14:36:51Z" uid="12551976" user="rms_ALG" changeset="133214654" lat="50.2148443" lon="11.9504193"/>
        </delete>
        <create>
            <node id="10704460411" version="1" timestamp="2023-03-02T14:37:02Z" uid="18525856" user="Matthew Warriner" changeset="133214661" lat="35.3632017" lon="-111.7347684">
              <tag k="access" v="yes"/>
              <tag k="amenity" v="parking"/>
            </node>
            <node id="10704460412" version="1" timestamp="2023-03-02T14:37:35Z" uid="18589376" user="Rumanah" changeset="133214679" lat="0.1387827" lon="34.6348208"/>
        </create>
        <modify>
            <way id="8353735" version="17" timestamp="2023-03-02T14:36:41Z" uid="10402229" user="te42kyfo" changeset="133214633">
              <nd ref="25300316"/>
              <nd ref="2137983311"/>
              <nd ref="2334329899"/>
              <nd ref="255463104"/>
              <nd ref="2334329905"/>
              <nd ref="25300314"/>
              <nd ref="2102071638"/>
              <nd ref="255463450"/>
              <nd ref="2102071652"/>
              <tag k="cycleway:both" v="no"/>
              <tag k="highway" v="residential"/>
              <tag k="maxspeed" v="30"/>
              <tag k="name" v="Langenaustraße"/>
              <tag k="sidewalk" v="right"/>
              <tag k="smoothness" v="good"/>
              <tag k="surface" v="asphalt"/>
              <tag k="width" v="6"/>
            </way>
        </modify>
        <delete>
            <way id="24584436" version="5" timestamp="2023-03-02T14:36:51Z" uid="12551976" user="rms_ALG" changeset="133214654"/>
        </delete>
        <create>
          <relation id="15549519" version="1" timestamp="2023-03-02T14:37:22Z" uid="172867" user="AmiFritz" changeset="133214669">
            <member type="way" ref="40776641" role="street"/>
            <member type="way" ref="749052050" role="street"/>
            <member type="node" ref="10704515502" role="house"/>
            <tag k="name" v="Rue Stendhal"/>
            <tag k="ref:FR:FANTOIR" v="870854950C"/>
            <tag k="type" v="associatedStreet"/>
          </relation>
        </create>
      </osmChange>""";


  @Test
  void entities() throws Exception {
    var tagTranslator = new CachedTagTranslator(new MemoryTagTranslator(), 1024);

    try ( var input = new ByteArrayInputStream(osc.getBytes());
          var osmSource = new OscParser(input)) {
      var entities = osmSource.entities(tagTranslator);
      var list = entities.flatMap(Tuple2::getT2)
          .collectList().block();
      assertEquals(7, list.size());

      var tagHighwayResidantial = tagTranslator.getOSHDBTagOf(new OSMTag("highway","residential"));
      System.out.println("tagHighwayResidantial = " + tagHighwayResidantial);

      var roleStreet = tagTranslator.getOSHDBRoleOf("street");
      System.out.println("roleStreet = " + roleStreet);

      list.stream()
          .forEach(osm -> System.out.printf("%s %s%n", osm, tagTranslator.lookupTag(osm.getTags())));
    }

    var sortedTags = new TreeMap<OSHDBTag, OSMTag>();
    sortedTags.putAll(tagTranslator.getLookupOSHDBTag().asMap());
    sortedTags.forEach((osm, oshdb) -> System.out.printf("%s -> %s%n", osm, oshdb));


  }
}