package org.heigit.bigspatialdata.oshdb.tool.updater;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to automatically download OSCData. It starts by searching the first
 * OSCDataset after the date of creation of the imported OSH-Full-History-Dump
 * and starts downloading data.
 *
 */
public class OSCDownloader implements Runnable {

  //variables  
  private static final Logger LOG = LoggerFactory.getLogger(OSCDownloader.class);
  public static final BlockingQueue<File> downfiles = new LinkedBlockingQueue<>(20);

  /**
   * Static method to transform an Integer of the sequential OSC-IDs to the
   * linkparts in the URL to that specific file.
   *
   * @param oscMinId Full integer defining th OSC-ID to break.
   * @return A HashMap containing three pairs: "mio":million part,
   * "tsd":thousand part and "ein":singlepart.
   */
  private static HashMap<String, Integer> oscNumBreaker(int oscMinId) {
    HashMap<String, Integer> numparts = new HashMap<>(3);
    numparts.put("mio", oscMinId / 1000000);
    numparts.put("tsd", (oscMinId - (numparts.get("mio") * 1000000)) / 1000);
    numparts.put("ein", oscMinId - (numparts.get("mio") * 1000000) - (numparts.get("tsd") * 1000));
    return numparts;
  }
  private Date fullHistoryStart;
  private Path downStore;
  private URL baseURL;
  private int currentOSCId;
  private Date currentOSCDate;
  private DateFormat sdfOSC;

  /**
   * Constructor of the class that automatically finds the first dataset after
   * OSH-Creation. This may take some time.
   *
   * @param CreationDate Creation Date so wie sie auf der Seite des full-history
   * steht (http://planet.openstreetmap.org/pbf/full-history/)
   * @param downPath The path to the directory where the downloaded OSC-data
   * will be stored temporarily.
   */
  public OSCDownloader(Date CreationDate, Path downPath) {
    this.fullHistoryStart = CreationDate;
    this.downStore = downPath;
    this.sdfOSC = new SimpleDateFormat("'timestamp='yyy-MM-dd'T'HH'\\:'mm'\\:'ss'Z'");
    try {
      this.baseURL = new URL("http://planet.openstreetmap.org/replication/minute");
      this.currentOSCId = this.startFinder();
    } catch (MalformedURLException ex) {
      LOG.error("This should not have happened! "
              + "Seems like the programmer made a mistake when hardcoding this "
              + "URL: http://planet.openstreetmap.org/replication/minute . Change"
              + "the sourcecode!");
    } catch (ParseException ex) {
      LOG.error("This is an error in the scourcecode. Check around line 71.");
      System.exit(1);
    } catch (IOException ex) {
      LOG.error("Maybe the location of timestamp information has changed."
              + "Anyway, I could not find it. Check if you are connected to the internet"
              + "or if the location of the changesets has changed (I tried "
              + "http://planet.openstreetmap.org/replication/minute but dit not succeed)");
      System.exit(1);
    }
    
  }
  
  private String linkConstructor(int oscMinId) {
    //URL website= new URL("http://planet.openstreetmap.org/replication/minute/002/169/");
    DecimalFormat myFormatter = new DecimalFormat("000");
    HashMap<String, Integer> numparts = oscNumBreaker(oscMinId);
    
    String relURL = "/" + myFormatter.format(numparts.get("mio")) + "/" + myFormatter.format(numparts.get("tsd")) + "/" + myFormatter.format(numparts.get("ein"));
    return this.baseURL + relURL;
    
  }
  
  private int startFinder() throws MalformedURLException, ParseException, IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy--HH-mm-ss");
    
    Date oscMinuteStart = sdf.parse("12-09-2012--08-15-45"); // Start of OSC Minutely updates
    int mindiff = (int) ((this.fullHistoryStart.getTime() - oscMinuteStart.getTime()) / 60000);
    
    this.currentOSCDate = this.getTimestampFromState(mindiff);
    
    while (this.currentOSCDate.before(this.fullHistoryStart)) {
      mindiff += 1;
      this.currentOSCDate = this.getTimestampFromState(mindiff);
    }
    return mindiff;
  }
  
  private void decompress(File downfile) {
    byte[] buffer = new byte[1024];
    FileOutputStream out;
    File decomp = new File(downfile.getAbsolutePath().replaceAll(".gz", ""));
    LOG.debug(decomp.toString());
    try (GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(downfile))) {
      out = new FileOutputStream(decomp);
      int len;
      while ((len = gzis.read(buffer)) > 0) {
        out.write(buffer, 0, len);
      }
      out.close();
      OSCDownloader.downfiles.put(decomp);
      downfile.delete();
    } catch (IOException ex) {
      LOG.error("There was a problem extracting the file.");
    } catch (InterruptedException ex) {
      LOG.error("There was a problem extracting the file. I was interrupted.");
    }
    
  }
  
  private Date getTimestampFromState(int oscId) throws MalformedURLException, IOException, ParseException {
    URL currurl;
    if (oscId == 0) {
      currurl = new URL(this.baseURL + "/state.txt");
    } else {
      currurl = new URL(linkConstructor(oscId) + ".state.txt");
    }
    String currdate = null;
    try (Scanner StateScanner = new Scanner(currurl.openStream())) {
      while (StateScanner.hasNextLine() && currdate == null) {
        currdate = StateScanner.findInLine("timestamp.*");
        StateScanner.nextLine();
      }
    }
    return sdfOSC.parse(currdate);
    
  }
  
  private void downloadFile() throws IOException, MalformedURLException, ParseException {
    File downfile = new File(this.downStore.toString(), this.currentOSCId + ".osc.gz");
    LOG.debug(downfile.toString());
    FileUtils.copyURLToFile(new URL(this.linkConstructor(this.currentOSCId) + ".osc.gz"), downfile);
    this.decompress(downfile);
    this.currentOSCId++;
    this.currentOSCDate = this.getTimestampFromState(currentOSCId);
  }
  
  @Override
  public void run() {
    try {
      Date nowOSCMinute = this.getTimestampFromState(0);

      //catch up to current state of updates
      while (this.currentOSCDate.before(nowOSCMinute)) {
        this.downloadFile();
        nowOSCMinute = this.getTimestampFromState(0);
      }

      //update every minute
      int downnr = 0;
      int callnumber = 0;
      while (true) {
        try {
          this.downloadFile();
          downnr++;
          LOG.info("Download NR: {0}", downnr);
        } catch (IOException ex) {
          LOG.error("Could not write file. Will start again later");
          try {
            Thread.sleep(54000);
          } catch (InterruptedException ex1) {
            LOG.error("The thread was interrupded. This is really bad!");
          }
          callnumber++;
          if (callnumber < 5) {
            return;
          } else {
            LOG.error("Timeout after 5 attemts");
            System.exit(1);
          }
        }
        try {
          Thread.sleep(54000);
        } catch (InterruptedException ex) {
          LOG.error("The thread was interrupded. This is really bad!");
        }
      }
    } catch (IOException | ParseException ex) {
      LOG.error(ex.getLocalizedMessage());
    }
  }
  
}
