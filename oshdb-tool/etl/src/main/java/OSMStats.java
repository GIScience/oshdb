import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.reactive.MyLambdaSubscriber;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.rx.Osh;
import org.heigit.bigspatialdata.oshpbf.parser.rx.RxOshPbfReader;
import org.reactivestreams.Publisher;
import com.google.common.base.Stopwatch;
import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.operators.flowable.FlowableBlockingSubscribe;
import org.roaringbitmap.RoaringBitmap;

public class OSMStats {

  private final Path pbf;

  private boolean isNODE = false;
  private boolean isWAY = false;
  private boolean isRELATION = false;


  private long globalNumberOfTags = 0;
  private long globalNumberOfEntities = 0;
  private long globalNumberOfVersions = 0;
  private List<Integer> numberOfTagsForHistogramm = new ArrayList<Integer>();

  private long globalMaxNumberOfTags = 0;
  private long globalMaxNumberOfTagsID;

  private long globalIDMin = 0;
  private long globalIDMax = 0;

  private long globalNumberOfVisibleEntities;

  private int globalNumberOfObjectsWithTags;

  private RoaringBitmap uniqueUser = new RoaringBitmap();


  public OSMStats(Path pbf) {
    this.pbf = pbf;
  }

  public void run() {
    Flowable<Osh> flow = RxOshPbfReader //
        .readOsh(pbf, 0, -1, -1) //
    // .filter(osh -> osh.getType() == OSMType.NODE)//
    // .limit(100)

    ;

    subscribe(flow, this::compute, this::error, this::done);

    printAndReset();

  }

  private void printAndReset() {

    System.out.println("number of entities: " + globalNumberOfEntities);
    System.out.println("number of all tags: " + globalNumberOfTags);
    System.out.println("number of all Versions: " + globalNumberOfVersions);
    System.out.println("Average number of Tags per Version: "
        + (double) globalNumberOfTags / globalNumberOfVersions);
    System.out.println("Average number of Versions per Entity: "
        + (double) globalNumberOfVersions / globalNumberOfEntities);
    System.out.println(
        "Max number of tags: ID " + globalMaxNumberOfTagsID + " NUMBER " + globalMaxNumberOfTags);

    for (Integer integer : numberOfTagsForHistogramm) {
      System.out.print(integer + " ");
    }
    System.out.println("");
    System.out.println("ID min: " + globalIDMin + " ID max: " + globalIDMax);
    System.out.println("number of visible entities: " + globalNumberOfVisibleEntities);
    System.out.println(
        "number of Objects with Tags (2 versions minimum): " + globalNumberOfObjectsWithTags);

    System.out.println("number of unique users: " + uniqueUser.getLongCardinality());

    System.out.println("-------------------");

    globalNumberOfEntities = 0;
    globalNumberOfTags = 0;
    globalNumberOfVersions = 0;
    globalMaxNumberOfTags = 0;
    globalMaxNumberOfTagsID = 0;

    numberOfTagsForHistogramm.clear();

    globalIDMin = 0;
    globalIDMax = 0;

    globalNumberOfVisibleEntities = 0;
    globalNumberOfObjectsWithTags = 0;

    uniqueUser.clear();

  }


  @SuppressWarnings("unchecked")
  public void compute(Osh osh) {
    switch (osh.getType()) {
      case NODE:
        if (isNODE == false) {
          isNODE = true;
          globalIDMin = osh.getId();
        }
        // nodes(osh.getId(), (List<Node>) (Object) osh.getVersions());
        entities(osh.getId(), (List<Entity>) (Object) osh.getVersions());

        break;
      case WAY:
        if (isWAY == false) {
          printAndReset();
          isNODE = false;
          isWAY = true;
          globalIDMin = osh.getId();

        }
        // ways(osh.getId(), (List<Way>) (Object) osh.getVersions());
        entities(osh.getId(), (List<Entity>) (Object) osh.getVersions());
        break;
      case RELATION:
        if (isRELATION == false) {
          printAndReset();
          isWAY = false;
          isRELATION = true;
          globalIDMin = osh.getId();
        }
        // relations(osh.getId(), (List<Relation>) (Object) osh.getVersions());
        entities(osh.getId(), (List<Entity>) (Object) osh.getVersions());
        break;
      default:
        System.err.println("unkown osm  type " + osh);
    }
  }



  private void entities(long id, List<Entity> versions) {
    int numberOfTags = computeNumberOfTags(versions, id);

    globalNumberOfVersions = globalNumberOfVersions + versions.size();
    globalNumberOfTags = globalNumberOfTags + numberOfTags;
    globalNumberOfEntities++;
    globalIDMax = id;

    // compute number of visible entities
    if (versions.get(versions.size() - 1).isVisible()) {
      globalNumberOfVisibleEntities++;
    }

    // compute number of osh objects (more then one version) with tags
    if (versions.size() > 1 && numberOfTags > 0) {
      globalNumberOfObjectsWithTags++;

    }


    // System.out.println(versions);
  }

  private int computeNumberOfTags(List<Entity> versions, long id) {
    int numberOfTags = 0;
    for (Entity entity : versions) {
      numberOfTags = computeVersionBased(id, numberOfTags, entity);
    }
    return numberOfTags;
  }

  // version based
  private int computeVersionBased(long id, int numberOfTags, Entity entity) {
    int tagsPerVersion = entity.getTags().length;
    numberOfTags = numberOfTags + tagsPerVersion;

    // compute maximum number of tags
    if (tagsPerVersion > globalMaxNumberOfTags) {
      globalMaxNumberOfTags = tagsPerVersion;
      globalMaxNumberOfTagsID = id;
    }

    // Histogramm
    int init = tagsPerVersion - numberOfTagsForHistogramm.size() + 1;
    for (int i = 0; i < init; i++)
      numberOfTagsForHistogramm.add(0);
    int element = numberOfTagsForHistogramm.get(tagsPerVersion) + 1;
    numberOfTagsForHistogramm.set(tagsPerVersion, element);

    // uniqueUser
    uniqueUser.add(entity.getUserId());



    return numberOfTags;
  }


  /*
   * private void nodes(long id, List<Node> versions) {
   * 
   * globalNumberOfVersions = globalNumberOfVersions + versions.size(); globalNumberOfTags =
   * globalNumberOfTags + computeNumberOfTagsNODE(versions, id); globalNumberOfEntrys++; globalIDMax
   * = id;
   * 
   * // System.out.println(versions); }
   * 
   * private void ways(long id, List<Way> versions) { globalNumberOfVersions =
   * globalNumberOfVersions + versions.size(); globalNumberOfTags = globalNumberOfTags +
   * computeNumberOfTagsWAY(versions, id); globalNumberOfEntrys++; globalIDMax = id;
   * 
   * // System.out.println(versions); }
   * 
   * private void relations(long id, List<Relation> versions) { globalNumberOfVersions =
   * globalNumberOfVersions + versions.size(); globalNumberOfTags = globalNumberOfTags +
   * computeNumberOfTagsRELATION(versions, id); globalNumberOfEntrys++; globalIDMax = id;
   * 
   * // System.out.println(versions); }
   */



  /*
   * private int computeNumberOfTagsNODE(List<Node> versions, long id) { int numberOfTags = 0; for
   * (Node node : versions) { numberOfTags = numberOfTagsVersion(id, numberOfTags, node); } return
   * numberOfTags; }
   * 
   * 
   * private int computeNumberOfTagsWAY(List<Way> versions, long id) { int numberOfTags = 0; for
   * (Way way : versions) { numberOfTags = numberOfTagsVersion(id, numberOfTags, way); } return
   * numberOfTags; }
   * 
   * private int computeNumberOfTagsRELATION(List<Relation> versions, long id) { int numberOfTags =
   * 0; for (Relation relation : versions) { numberOfTags = numberOfTagsVersion(id, numberOfTags,
   * relation); } return numberOfTags; }
   */



  public void error(Throwable e) {
    // react an error
    e.printStackTrace();
  }

  public void done() {
    // clean up

  }

  public static void main(String[] args) {
    Path dir = Paths.get("C:\\Users\\Danda\\heigit\\data");
    Path pbf;

    pbf = dir.resolve("heidelberg.osh.pbf");

    OSMStats osmStats = new OSMStats(pbf);



    Stopwatch stopwatch = Stopwatch.createStarted();
    osmStats.run();
    System.out.println(stopwatch);



  }



  private static <T> void subscribe(Publisher<? extends T> o, final Consumer<? super T> onNext,
      final Consumer<? super Throwable> onError, final Action onComplete) {
    ObjectHelper.requireNonNull(onNext, "onNext is null");
    ObjectHelper.requireNonNull(onError, "onError is null");
    ObjectHelper.requireNonNull(onComplete, "onComplete is null");
    FlowableBlockingSubscribe.subscribe(o,
        new MyLambdaSubscriber<T>(onNext, onError, onComplete, 1L));
  }
}
