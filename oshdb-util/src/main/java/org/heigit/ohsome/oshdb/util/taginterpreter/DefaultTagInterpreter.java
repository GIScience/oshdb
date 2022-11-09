package org.heigit.ohsome.oshdb.util.taginterpreter;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link TagInterpreter} implementation.
 */
public class DefaultTagInterpreter extends BaseTagInterpreter {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTagInterpreter.class);

  private int typeKey;
  private OSHDBTag typeMultipolygon;
  private OSHDBTag typeBoundary;
  private OSHDBTag typeRoute;

  private static final String defaultAreaTagsDefinitionFile = "json/polygon-features.json";
  private static final String defaultUninterestingTagsDefinitionFile
      = "json/uninterestingTags.json";

  /**
   * Constructor using given {@link TagTranslator} and default values as areaTagsDefinitonFile and
   * uninterestingTagsDefinitionFile.
   *
   * <p>
   *   Details see
   *   {@link DefaultTagInterpreter#DefaultTagInterpreter(TagTranslator, String, String)}.
   * </p>
   */
  public DefaultTagInterpreter(TagTranslator tagTranslator) throws IOException, ParseException {
    this(
        tagTranslator,
        defaultAreaTagsDefinitionFile,
        defaultUninterestingTagsDefinitionFile
    );
  }

  /**
   * Constructor using given {@link TagTranslator}, areaTagsDefinitonFile, and
   * uninterestingTagsDefinitionFile.
   *
   * @param tagTranslator {@link TagTranslator} used by {@link TagInterpreter}
   * @param areaTagsDefinitionFile filename of a JSON file containing tags that are supposed to be
   *                               areas
   * @param uninterestingTagsDefinitionFile filename of a JSON file containing tags to be ignored
   * @throws IOException thrown for all IO read/write errors
   * @throws ParseException for parsing errors
   */
  public DefaultTagInterpreter(
      TagTranslator tagTranslator,
      String areaTagsDefinitionFile, String uninterestingTagsDefinitionFile
  ) throws IOException, ParseException {
    super(new OSHDBTag(-1, -1), null, null, null, -1, -1, -1); // initialize with dummy parameters for now
    // construct list of area tags for ways
    Map<Integer, Set<Integer>> wayAreaTags = new HashMap<>();

    JSONParser parser = new JSONParser();
    JSONArray tagList = (JSONArray) parser.parse(new InputStreamReader(
        Thread.currentThread().getContextClassLoader().getResourceAsStream(areaTagsDefinitionFile)
    ));
    // todo: check json schema for validity

    @SuppressWarnings("unchecked") // we expect only JSON objects here in a valid definition file
    Iterable<JSONObject> iterableTagList = tagList;
    for (JSONObject tag : iterableTagList) {
      String key = (String) tag.get("key");
      switch ((String) tag.get("polygon")) {
        case "all":
          Set<Integer> valueIds = new InvertedHashSet<>();
          tagTranslator.getOSHDBTagKeyOf(key).map(OSHDBTagKey::toInt).ifPresent(keyId -> {
            tagTranslator.getOSHDBTagOf(key, "no").map(OSHDBTag::getValue).ifPresent(valueIds::add);
            wayAreaTags.put(keyId, valueIds);
          });
          break;
        case "whitelist":
          valueIds = new HashSet<>();
          tagTranslator.getOSHDBTagKeyOf(key).map(OSHDBTagKey::toInt).ifPresent(keyId -> {
            JSONArray values = (JSONArray) tag.get("values");
            @SuppressWarnings("unchecked") // we expect only strings here in a valid definition file
            Iterable<String> iterableWhitelistValues = values;
            for (String value : iterableWhitelistValues) {
              OSMTag keyValue = new OSMTag(key, value);
              tagTranslator.getOSHDBTagOf(keyValue).map(OSHDBTag::getValue)
                  .ifPresent(valueIds::add);
            }
            tagTranslator.getOSHDBTagOf(key, "no").map(OSHDBTag::getValue).ifPresent(valueIds::add);
            wayAreaTags.put(keyId, valueIds); });
          break;
        case "blacklist":
          valueIds = new InvertedHashSet<>();
          tagTranslator.getOSHDBTagKeyOf(key).map(OSHDBTagKey::toInt).ifPresent(keyId -> {
            JSONArray values = (JSONArray) tag.get("values");
            @SuppressWarnings("unchecked") // we expect only strings here in a valid definition file
            Iterable<String> iterableBlacklistValues = values;
            for (String value : iterableBlacklistValues) {
              OSMTag keyValue = new OSMTag(key, value);
              tagTranslator.getOSHDBTagOf(keyValue).map(OSHDBTag::getValue)
                  .ifPresent(valueIds::add);
            }
            wayAreaTags.put(keyId, valueIds); });
          break;
        default:
          throw new ParseException(-13);
      }
    }

    // hardcoded type=multipolygon for relations
    this.typeKey = tagTranslator.getOSHDBTagKeyOf("type").map(OSHDBTagKey::toInt).orElse(-1);
    this.typeMultipolygon = tagTranslator.getOSHDBTagOf("type", "multipolygon").orElse(new OSHDBTag(typeKey, -1));
    this.typeBoundary = tagTranslator.getOSHDBTagOf("type", "boundary").orElse(new OSHDBTag(typeKey, -2));
    this.typeRoute = tagTranslator.getOSHDBTagOf("type", "route").orElse(new OSHDBTag(typeKey, -3));

    // we still need to also store relation area tags for isOldStyleMultipolygon() functionality!
    Map<Integer, Set<Integer>> relAreaTags = new TreeMap<>();
    Set<Integer> relAreaTagValues = new TreeSet<>();
    relAreaTagValues.add(this.typeMultipolygon.getValue());
    relAreaTagValues.add(this.typeBoundary.getValue());
    relAreaTags.put(this.typeKey, relAreaTagValues);

    // list of uninteresting tags
    Set<Integer> uninterestingTagKeys = new HashSet<>();
    JSONArray uninterestingTagsList = (JSONArray) parser.parse(new InputStreamReader(
        Thread.currentThread().getContextClassLoader().getResourceAsStream(
            uninterestingTagsDefinitionFile)));
    // todo: check json schema for validity

    @SuppressWarnings("unchecked") // we expect only strings here in a valid definition file
    Iterable<String> iterableUninterestingTagsList = uninterestingTagsList;
    for (String tagKey : iterableUninterestingTagsList) {
      tagTranslator.getOSHDBTagKeyOf(tagKey).map(OSHDBTagKey::toInt).ifPresent(uninterestingTagKeys::add);
    }

    this.wayAreaTags = wayAreaTags;
    this.relationAreaTags = relAreaTags;
    this.uninterestingTagKeys = uninterestingTagKeys;

    var areaTagKey = tagTranslator.getOSHDBTagKeyOf("area").map(OSHDBTagKey::toInt).orElse(-2);
    this.areaNo = tagTranslator.getOSHDBTagOf("area", "no").orElse(new OSHDBTag(areaTagKey, -1));

    this.outerRoleId = tagTranslator.getOSHDBRoleOf("outer").map(OSHDBRole::getId).orElse(-1);
    this.innerRoleId = tagTranslator.getOSHDBRoleOf("inner").map(OSHDBRole::getId).orElse(-2);
    this.emptyRoleId = tagTranslator.getOSHDBRoleOf("").map(OSHDBRole::getId).orElse(-3);
  }

  @Override
  public boolean isArea(OSMEntity entity) {
    if (entity instanceof OSMRelation) {
      return evaluateRelationForArea((OSMRelation) entity);
    } else {
      return super.isArea(entity);
    }
  }

  @Override
  public boolean isLine(OSMEntity entity) {
    if (entity instanceof OSMRelation) {
      return evaluateRelationForLine((OSMRelation) entity);
    } else {
      return super.isLine(entity);
    }
  }

  // checks if the relation has the tag "type=multipolygon"
  @Override
  protected boolean evaluateRelationForArea(OSMRelation entity) {
    var tags = entity.getTags();
    // skip area=no check, since that doesn't make much sense for multipolygon relations (does it??)
    // the following is slightly faster than running
    // `return entity.hasTagValue(k1,v1) || entity.hasTagValue(k2,v2);`
    for (var tag : tags) {
      if (tag.getKey() == typeKey) {
        return  tag.equals(typeMultipolygon) || tag.equals(typeBoundary);
      }
    }
    return false;
  }

  // checks if the relation has the tag "type=route"
  private boolean evaluateRelationForLine(OSMRelation entity) {
    return entity.getTags().hasTag(typeRoute);
  }
}
