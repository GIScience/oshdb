package org.heigit.ohsome.oshdb.api.db;

import com.google.common.base.Joiner;
import java.io.File;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteRunnable;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.MapReducerIgniteAffinityCall;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.MapReducerIgniteLocalPeek;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.MapReducerIgniteScanQuery;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTableNotFoundException;
import org.heigit.ohsome.oshdb.util.mappable.OSHDBMapReducible;
import org.heigit.ohsome.oshdb.util.tagtranslator.JdbcTagTranslator;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;

/**
 * OSHDB database backend connector to a Ignite system.
 */
public class OSHDBIgnite extends OSHDBDatabase implements AutoCloseable {
  /**
   * Specifies which algorithm to use when running OSHDB queries on Ignite.
   *
   * <p>Available computation modes:
   * <ul>
   *   <li>{@link #LOCAL_PEEK} (default) is good for small to medium sized areas
   *   <li>{@link #SCAN_QUERY} works best for large to global queries
   *   <li>{@link #AFFINITY_CALL} is useful for streaming queries
   * </ul>
   */
  public enum ComputeMode {
    LOCAL_PEEK,
    SCAN_QUERY,
    AFFINITY_CALL
  }

  private final Ignite ignite;
  private final TagTranslator tagTranslator;
  private final boolean owner;
  private ComputeMode computeMode = ComputeMode.LOCAL_PEEK;

  private IgniteRunnable onCloseCallback = null;

  /**
   * Create a new OSHDBDatabase based on default ("ignite-config.xml") configuration.
   *
   * @param prefix Prefix for cache/table names
   * @param keytables DataSource for external Keytables
   * @throws OSHDBException if cluster state is not active.
   */
  public OSHDBIgnite(String prefix, DataSource keytables) {
    this(new File("ignite-config.xml"), prefix, keytables);
  }

  /**
   * Creates a new OSHDBDatabase using the given Ignite instance.
   *
   * @param ignite Ignite instance to use.
   * @param prefix Prefix for cache/table names
   * @param keytables DataSource for external Keytables
   * @throws OSHDBException if cluster state is not active.
   */
  public OSHDBIgnite(Ignite ignite, String prefix, DataSource keytables) {
    this(ignite, false, prefix, keytables);
  }

  /**
   * Opens a connection to oshdb data stored on an Ignite cluster.
   *
   * @param igniteConfigFilePath ignite configuration file
   * @param prefix Prefix for cache/table names
   * @param keytables DataSource for external Keytables
   * @throws OSHDBException if cluster state is not active.
   */
  public OSHDBIgnite(String igniteConfigFilePath, String prefix, DataSource keytables) {
    this(new File(igniteConfigFilePath), prefix, keytables);
  }

  /**
   * Opens a connection to oshdb data stored on an Ignite cluster.
   *
   * @param igniteConfig ignite configuration file
   * @param prefix Prefix for cache/table names
   * @param keytables DataSource for external Keytables
   * @throws OSHDBException if cluster state is not active.
   */
  public OSHDBIgnite(File igniteConfig, String prefix, DataSource keytables) {
    this(startClient(igniteConfig), true, prefix, keytables);
  }

  private OSHDBIgnite(Ignite ignite, boolean owner, String prefix, DataSource keytables) {
    super(prefix);
    this.ignite = ignite;
    this.owner = owner;
    this.tagTranslator = new JdbcTagTranslator(keytables);

    checkStateActive();
  }

  private static Ignite startClient(File igniteConfig) {
    Ignition.setClientMode(true);
    return Ignition.start(igniteConfig.toString());
  }

  private void checkStateActive() {
    var cluster = ignite.cluster();
    if (!cluster.state().active()) {
      throw new OSHDBException("cluster not in state active!");
    }
  }

  @Override
  public TagTranslator getTagTranslator() {
    return tagTranslator;
  }

  @Override
  public <X extends OSHDBMapReducible> MapReducer<X> createMapReducer(Class<X> forClass) {
    MapReducer<X> mapReducer;
    Collection<String> allCaches = this.getIgnite().cacheNames();
    Collection<String> expectedCaches = Stream.of(OSMType.values())
        .map(TableNames::forOSMType).filter(Optional::isPresent).map(Optional::get)
        .map(t -> t.toString(prefix))
        .collect(Collectors.toList());
    if (!allCaches.containsAll(expectedCaches)) {
      throw new OSHDBTableNotFoundException(Joiner.on(", ").join(expectedCaches));
    }
    switch (this.computeMode()) {
      case LOCAL_PEEK:
        mapReducer = new MapReducerIgniteLocalPeek<>(this, forClass);
        break;
      case SCAN_QUERY:
        mapReducer = new MapReducerIgniteScanQuery<>(this, forClass);
        break;
      case AFFINITY_CALL:
        mapReducer = new MapReducerIgniteAffinityCall<>(this, forClass);
        break;
      default:
        throw new UnsupportedOperationException("Backend not implemented for this compute mode.");
    }
    return mapReducer;
  }

  @Override
  public String metadata(String property) {
    // todo: implement this
    return null;
  }

  /**
   * Returns the actual Ignite instance.
   *
   * @return Ignite instance
   */
  public Ignite getIgnite() {
    return this.ignite;
  }

  @Override
  public void close() {
    if (owner) {
      this.ignite.close();
    }
  }

  /**
   * Sets the compute mode.
   *
   * @param computeMode the compute mode to be used in calculations on this oshdb backend
   * @return this backend
   */
  public OSHDBIgnite computeMode(ComputeMode computeMode) {
    this.computeMode = computeMode;
    return this;
  }

  /**
   * Gets the set compute mode.
   *
   * @return the currently set compute mode
   */
  public ComputeMode computeMode() {
    return this.computeMode;
  }

  /**
   * Sets a callback to be executed on all ignite workers after the query has been finished.
   *
   * <p>This can be used to close connections to (temporary) databases that were used to store or
   * retrieve intermediate data.</p>
   *
   * @param action the callback to execute after a query is done
   * @return the current oshdb database object
   */
  public OSHDBIgnite onClose(IgniteRunnable action) {
    this.onCloseCallback = action;
    return this;
  }

  /**
   * Gets the onClose callback.
   *
   * @return the currently set onClose callback
   */
  public Optional<IgniteRunnable> onClose() {
    return Optional.ofNullable(this.onCloseCallback);
  }

}
