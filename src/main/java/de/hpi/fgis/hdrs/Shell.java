/**
 * Copyright 2011 Daniel Hefenbrock
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.hpi.fgis.hdrs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.ipc.RPC;

import de.hpi.fgis.hdrs.client.Client;
import de.hpi.fgis.hdrs.client.ClientSegmentScanner;
import de.hpi.fgis.hdrs.ipc.NodeProtocol;
import de.hpi.fgis.hdrs.node.NodeStatus;
import de.hpi.fgis.hdrs.node.SegmentSummary;
import de.hpi.fgis.hdrs.routing.SegmentInfo;
import de.hpi.fgis.hdrs.segment.Segment.SegmentSize;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.triplefile.TripleFileInfo;


public class Shell {

  private static String PROMPT = "> ";
  
  private final PrintStream out = System.out;
  private final InputStream in = System.in;
  
  private final Configuration conf;
  private final ArrayList<Peer> peers;
  private final Set<Triple.COLLATION> indexes;
  private final Map<String, Command> commands;
  private final List<String> cmdlist;
  
  
  private String prompt = "";
  private Peer peer = null;
  
  
  public void run() throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(this.in));
    
    String line;
    do {
      out.print(prompt + PROMPT);
      line = in.readLine();
      if (null == line) {
        break;
      }
    } while (run(line.trim()));
  }
  
  
  boolean run(String line) throws IOException {
    String cmd;
    String params = "";
    if (-1 == line.indexOf(' ')) {
      cmd = line;
    } else {
      cmd = line.substring(0, line.indexOf(' '));
      params = line.substring(line.indexOf(' ')).trim();
    } 
    Command command = commands.get(cmd);
    if (null == command) {
      out.println("unknown command '" + cmd + "'");
      return true;
    }
    if (command.check()) {
      command.run(params);
    }
    return !command.quit();
  }
  
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    Configuration conf = Configuration.create();
    ArrayList<Peer> peers;
    Set<Triple.COLLATION> indexes;
    if (2 == args.length) {
      peers = Configuration.readPeers(args[0]);
      indexes = Configuration.readIndexes(args[1]);
    } else {
      System.out.print("enter store address: ");
      String address = (new BufferedReader(new InputStreamReader(System.in))).readLine();
      StoreInfo info = Configuration.loadStoreInfo(conf, address);
      peers = info.getPeers();
      indexes = info.getIndexes();
    }
    Shell shell = new Shell(conf, peers, indexes);
    shell.run();
  }
  
  
  public Shell(Configuration conf, ArrayList<Peer> peers, Set<Triple.COLLATION> indexes) {
    this.conf = conf;
    this.peers = peers;
    this.indexes = indexes;
    commands = new HashMap<String, Command>();
    cmdlist = new ArrayList<String>();
    initCommands();
  }
  
  
  private void initCommands() {
    addComand(new Help(), "help", "h", "?");
    addComand(new Exit(), "exit", "quit", "q");
    addComand(new Peers(), "peers", "p");
    addComand(new All(), "all");
    addComand(new SelectPeer(), "peer");
    addComand(new Index(), "index", "i");
    addComand(new Segments(), "segments", "s");
    //addComand(new View(), "view", "v");
    addComand(new OpenSegment(), "open", "o");
    addComand(new Segment(), "segment", "se");
    addComand(new CompactSegment(), "compact");
    addComand(new FlushSegment(), "flush");
    addComand(new Restart(), "restart");
    addComand(new Shutdown(), "shutdown");
    addComand(new Status(), "status", "st");
    addComand(new SplitSegment(), "split");
    addComand(new Cat(), "cat");
    addComand(new Query(), "query");
  }
  
  private void addComand(Command cmd, String...name) {
    for (int i=0; i<name.length; ++i) {
      commands.put(name[i], cmd);
    }
    cmdlist.add(name[0]);
  }
  
  private abstract class Command {
    abstract void run(String params) throws IOException;
    boolean quit() {
      return false;
    }
    boolean check() throws IOException {
      return true;
    }
    String params() {
      return "";
    }
    String info() {
      return "";
    }
  }
  
  private class Exit extends Command {
    @Override
    void run(String params) {
      out.println("bye.");
    }
    @Override 
    boolean quit() {
      return true;
    }
    @Override
    String info() {
      return "Exit from HDRS Shell.";
    }
  }
  
  private class Help extends Command {
    @Override
    void run(String params) {
      out.println("HDRS Shell");
      out.println();
      out.println("Commands:");
      for (String name : cmdlist) {
        Command command = commands.get(name);
        System.out.println(String.format("%-27s %s", 
            (command instanceof PeerCommand ? "  " : " ") + name + " " + command.params(), 
            command.info()));
      }
    }
    @Override
    String info() {
      return "Show help.";
    }
  }
  
  private class Peers extends Command {
    @Override
    void run(String params) {
      for (int i=0; i<peers.size(); ++i) {
        out.println("Peer " + peers.get(i));
      }
      out.println(peers.size() + " peers.");
    }
    @Override
    String info() {
      return "List peers.";
    }
  }
  
  private class SelectPeer extends Command {
    @Override
    void run(String params) {
      if ("".equals(params)) {
        out.println("usage: 'peer " + params() + "'");
        return;
      }
      Peer peer = null;
      try {
        int peerId = Integer.parseInt(params);
        peer = peers.get(peerId-1);
      } catch (NumberFormatException ex) {
        out.println("Invalid peer id");
        return;
      } catch (IndexOutOfBoundsException ex) {
        out.println("Invalid peer id");
        return;
      }
      Shell.this.peer = peer;
      prompt = "Peer " + peer.getId();
      out.println("Peer " + peer + " selected.");
    }
    @Override
    String params() {
      return "<PeerId>";
    }
    @Override
    String info() {
      return "Select a peer to run subsequent commands on.";
    }
  }
  
  private abstract class PeerCommand extends Command {
    private NodeProtocol proxy = null;
    @Override
    boolean check() throws IOException {
      if (null == peer) {
        out.println("select peer first.");
        return false;
      }
      proxy = peer.getProxy(conf);
      return true;
    }
    @Override 
    boolean quit() {
      RPC.stopProxy(proxy);
      proxy = null;
      return super.quit();
    }
    protected NodeProtocol proxy() {
      return proxy;
    }
  }
  
  private class Index extends PeerCommand {
    @Override
    void run(String params) throws IOException {
      Triple.COLLATION order = null;
      if ("".equals(params)) {
        order = Triple.COLLATION.SPO;
      } else {
        order = parseOrder(params);
        if (null == order) {
          out.println("illegal collation.");
          return;
        }
      }
      SegmentSummary[] index = proxy().getIndexSummary(order.getCode());
      if (null == index) {
        out.println("index " + order + " does not exist");
        return;
      }
      out.println("Index " + order + " as seen by peer " + peer + ": ");
      
      out.println(String.format("%3s %-11s %6s %s", 
          "#", "SegmentID", "PeerID", "First Triple"));
      
      Triple prev = Triple.MAGIC_TRIPLE;
      for (int i=0; i<index.length; ++i) {
        SegmentInfo info = index[i].getInfo();
        if (!prev.equals(info.getLowTriple())) {
          out.println("? " + printTriple(prev));
        }
        prev = info.getHighTriple();
        out.println(String.format("%3d %11d %6d %s", 
            i+1,
            info.getSegmentId(),
            index[i].getPeerId(),
            printTriple(info.getLowTriple())));
      }
      if (!prev.equals(Triple.MAGIC_TRIPLE)) {
        out.println("? " + printTriple(prev));
      }
      out.println(index.length + " segment(s) in index.");
    }
    @Override
    String params() {
      return "[SPO, SOP, ...]";
    }
    @Override
    String info() {
      return "List segments in index as seen by selected peer.";
    }
  }
  
  private class Segments extends PeerCommand {
    @Override
    void run(String params) throws IOException {
      SegmentSummary[] segments = proxy().getPeerSummary();
      out.println("Segments stored at peer " + peer + ": ");
      
      out.println(String.format("%3s %-11s %3s %-13s %1s %2s %5s %6s %6s %6s %5s %s", 
          "#", "SegmentID", "Idx", "Status", "", "#F", "c.MB", "u.MB", "t.MB", "Idx.MB",
          "#T", "First Triple"));
      
      int totalNFiles = 0;
      long totalSize = 0;
      long totalUncompressedSize = 0;
      long totalDataSize = 0;
      long totalIndexSize = 0;
      long totalTriples = 0;
      long totalBuffer = 0;
      boolean incomplete = false;
      
      for (int i=0; i<segments.length; ++i) {
        SegmentInfo info = segments[i].getInfo();
        SegmentSize size = segments[i].getSize();
        out.println(String.format("%3d %11d %3s %-13s %1s %2s %5s %6s %6s %6s %5s %s", 
            i+1,
            info.getSegmentId(),
            segments[i].getIndex(),
            segments[i].getStatus().toString(),
            segments[i].getCompacting() ? 'C' : ' ',
            null == size
                ? "?" : String.format("%2d", size.getNumberOfFiles()),
            null == size
                ? "?" : String.format("%3.1f", LogFormatUtil.MB(size.getFileSize())),
            null == size
                ? "?" : String.format("%4.1f", LogFormatUtil.MB(size.getUncompressedFileSize())),
            null == size
                ? "?" : String.format("%4.1f", LogFormatUtil.MB(size.getDataSize())),
            null == size
                ? "?" : String.format("%3.1f", LogFormatUtil.MB(size.getIndexSize())),
            null == size
                ? "?" : printLargeNumber(size.getNumberOfTriples()),
            printTriple(info.getLowTriple())));
        if (null != size) {
          totalNFiles += size.getNumberOfFiles();
          totalSize += size.getFileSize();
          totalUncompressedSize += size.getUncompressedFileSize();
          totalDataSize += size.getDataSize();
          totalIndexSize += size.getIndexSize();
          totalTriples += size.getNumberOfTriples();
          totalBuffer += size.getBufferSize();
        } else {
          incomplete = true;
        }
      }
      if (!incomplete) {
        out.println(String.format("Totals: %s triples, %d segement(s), %d triplefile(s)\n" +
        		"        %.1f MB on disk (%.1f MB uncompressed, %.1f MB triple data)\n" +
        		"        %.1f MB buffer, %.1f MB index (%.1f MB in memory)", 
        		printLargeNumber(totalTriples),
        		segments.length,
        		totalNFiles,
        		LogFormatUtil.MB(totalSize),
        		LogFormatUtil.MB(totalUncompressedSize),
        		LogFormatUtil.MB(totalDataSize),
        		LogFormatUtil.MB(totalBuffer),
        		LogFormatUtil.MB(totalIndexSize),
        		LogFormatUtil.MB(totalBuffer + totalIndexSize)));
      } else {
        out.println(String.format(
            "Totals: %d segement(s), %.1f MB buffer, %.1f MB index (%.1f MB in memory)",
            segments.length, 
            LogFormatUtil.MB(totalBuffer),
            LogFormatUtil.MB(totalIndexSize),
            LogFormatUtil.MB(totalBuffer + totalIndexSize)));
      }
    }
    @Override
    String info() {
      return "List segments stored on selected peer.";
    }
  }
  
  private class Segment extends PeerCommand {
    @Override
    void run(String params) throws IOException {
      if ("".equals(params)) {
        out.println("usage: 'segment " + params() + "'");
        return;
      }
      long segmentId = 0;
      try {
        segmentId = Long.parseLong(params);
      } catch (NumberFormatException ex) {
        out.println("Invalid segment id");
        return;
      }
      SegmentSummary segment = proxy().getSegmentSummary(segmentId);
      if (null == segment) {
        out.println("Couldn't retrieve segment info");
        return;
      }
      
      Arrays.sort(segment.getFileInfo(), new Comparator<TripleFileInfo>() {
        @Override
        public int compare(TripleFileInfo t1, TripleFileInfo t2) {
          return (int) (t2.getSize() - t1.getSize());
        }
      });
      
      out.println("Status of Segment " + segmentId + ":");
      out.println(String.format("  Index:   %-10s Size:   %6.1f MB   u.Size: %6.1f MB", 
          segment.getIndex(), 
          LogFormatUtil.MB(segment.getSize().getFileSize()),
          LogFormatUtil.MB(segment.getSize().getUncompressedFileSize())));
      out.println(String.format("  Status:  %-10s Buffer: %6.1f MB   t.Size: %6.1f MB",
          segment.getStatus(),
          LogFormatUtil.MB(segment.getSize().getBufferSize()),
          LogFormatUtil.MB(segment.getSize().getDataSize())));
      out.println(String.format("  Triples: %s",
          printLargeNumber(segment.getSize().getNumberOfTriples())));
          
      out.println("Triplefiles:");
      
      out.println(String.format("%3s %5s %6s %6s %6s %1s %5s %s",
          '#', "c.MB", "u.MB", "t.MB", "Idx.MB", ' ', "#T", "Compression"));
      for (int i=0; i<segment.getFileInfo().length; ++i) {
        TripleFileInfo info = segment.getFileInfo()[i];
        out.println(String.format("%3d %5.1f %6.1f %6.1f %6.3f %1s %5s %s",
            i+1,
            LogFormatUtil.MB(info.getSize()),
            LogFormatUtil.MB(info.getUncompressedSize()),
            LogFormatUtil.MB(info.getUncompressedDataSize()),
            LogFormatUtil.MB(info.getIndexSize()),
            info.isHalfFile() ? 'H' : ' ',
            printLargeNumber(info.getNumberOfTriples()),
            info.getCompression()
            ));
      }
    }
    @Override
    String params() {
      return "<SegmentId>";
    }
    @Override
    String info() {
      return "Show details about a segment.";
    }
  }
  
  /*private class View extends PeerCommand {
    @Override
    void run(String params) throws IOException {
      PeerView[] view = proxy().getView();
      out.println("Peer view of peer " + peer + ": ");
      for (int i=0; i<view.length; ++i) {
        out.println(String.format("%2d %4d", 
            view[i].getPeerId(), view[i].getCatalogVersion()));
      }
    }
  }*/
  
  private class OpenSegment extends PeerCommand {
    @Override
    void run(String params) throws IOException {
      if ("".equals(params)) {
        out.println("usage: 'open " + params() + "'");
        return;
      }
      long segmentId = 0;
      if (!"*".equals(params)) {
        try {
          segmentId = Long.parseLong(params);
        } catch (NumberFormatException ex) {
          out.println("Invalid segment id");
          return;
        }
      }
      if (!proxy().openSegment(segmentId)) {
        out.println("Couldn't open segment.");
      } 
    }
    @Override
    String params() {
      return "<SegmentId>|*";
    }
    @Override
    String info() {
      return "Open a/all segment(s).";
    }
  }
  
  private class CompactSegment extends PeerCommand {
    @Override
    void run(String params) throws IOException {
      if ("".equals(params)) {
        out.println("usage: 'compact " + params() + "'");
        return;
      }
      long segmentId = 0;
      if (!"*".equals(params)) {
        try {
          segmentId = Long.parseLong(params);
        } catch (NumberFormatException ex) {
          out.println("Invalid segment id");
          return;
        }
      }
      if (!proxy().requestCompaction(segmentId)) {
        out.println("Couldn't compact segment.");
      } 
    }
    @Override
    String params() {
      return "<SegmentId>|*";
    }
    @Override
    String info() {
      return "Request compaction for a/all segment(s).";
    }
  }
  
  private class FlushSegment extends PeerCommand {
    @Override
    void run(String params) throws IOException {
      if ("".equals(params)) {
        out.println("usage: 'flush " + params() + "'");
        return;
      }
      long segmentId = 0;
      if (!"*".equals(params)) {
        try {
          segmentId = Long.parseLong(params);
        } catch (NumberFormatException ex) {
          out.println("Invalid segment id");
          return;
        }
      }
      if (!proxy().requestFlush(segmentId)) {
        out.println("Couldn't flush segment.");
      } 
    }
    @Override
    String params() {
      return "<SegmentId>|*";
    }
    @Override
    String info() {
      return "Request flush for a/all segment(s).";
    }
  }
  
  private class SplitSegment extends PeerCommand {
    @Override
    void run(String params) throws IOException {
      if ("".equals(params)) {
        out.println("usage: 'split " + params() + "'");
        return;
      }
      long segmentId = 0;
      try {
        segmentId = Long.parseLong(params);
      } catch (NumberFormatException ex) {
        out.println("Invalid segment id");
        return;
      }
      if (!proxy().requestSplit(segmentId)) {
        out.println("Couldn't split segment.");
      } 
    }
    @Override
    String params() {
      return "<SegmentId>";
    }
    @Override
    String info() {
      return "Request split for a/all segment(s).";
    }
  }
  
  private class Cat extends PeerCommand {
    @Override
    void run(String params) throws IOException {
      if ("".equals(params)) {
        out.println("usage: 'cat " + params() + "'");
        return;
      }
      String[] args = params.split(" ", 2);
      long segmentId = 0;
      try {
        segmentId = Long.parseLong(args[0]);
      } catch (NumberFormatException ex) {
        out.println("Invalid segment id");
        return;
      }
      int limit = 20;
      if (2 == args.length) {
        try {
          limit = Integer.parseInt(args[1]);
        } catch (NumberFormatException ex) {
          out.println("Invalid limit");
          return;
        }
      }
      ClientSegmentScanner scanner = ClientSegmentScanner.open(
          conf, segmentId, proxy(), null, false, null);
      while (scanner.next() && 0 <= limit--) {
        Triple t = scanner.pop();
        out.println(t);
      }
      scanner.close();
    }
    @Override
    String params() {
      return "<SegmentId> [<limit>]";
    }
    @Override
    String info() {
      return "Print first <limit> triples of segment <segmentId>.";
    }
  }
  
  private class All extends Command {
    @Override
    void run(String params) throws IOException {
      if ("".equals(params)) {
        out.println("usage: 'all " + params() + "'");
        return;
      }
      Peer p = Shell.this.peer;
      for (Peer peer : peers) {
        Shell.this.peer = peer;
        Shell.this.run(params);
      }
      Shell.this.peer = p;
    }
    @Override
    String params() {
      return "<Command>";
    }
    @Override
    String info() {
      return "Run a command on all peers.";
    }
  }
  
  private class Shutdown extends PeerCommand {
    @Override
    void run(String params) throws IOException {
      boolean force = "force".equals(params);
      proxy().shutDown(force);
      out.println("Peer " + peer + " shutting down...");
    }
    @Override
    String params() {
      return "[force]";
    }
    @Override
    String info() {
      return "Shutdown selected peer.";
    }
  }
  
  private class Restart extends PeerCommand {
    @Override
    void run(String params) throws IOException {
      boolean force = "force".equals(params);
      proxy().restart(force);
      out.println("Peer " + peer + " restarting...");
    }
    @Override
    String params() {
      return "[force]";
    }
    @Override
    String info() {
      return "Restart selected peer.";
    }
  }
  
  private class Status extends PeerCommand {
    @Override
    void run(String params) throws IOException {
      NodeStatus status = proxy().getNodeStatus();
      out.println(String.format("Peer %2d: %3d S %7.2f MB buf | %8.2f MB heap | " +
      		"%2d T %5.2f MB | %2d Sc",
      		peer.getId(),
      		status.getNumberOfSegments(),
      		LogFormatUtil.MB(status.getSegmentBuffer()),
          LogFormatUtil.MB(status.getHeapSpace()),
          status.getNumberOfTransactions(),
          LogFormatUtil.MB(status.getTransactionBuffer()),
          status.getNumberOfScanners()));
    }
    @Override
    String info() {
      return "Show status of selected peer.";
    }
  }
  
  private class Query extends Command {
    @Override
    void run(String params) throws IOException {
      if ("".equals(params)) {
        out.println("usage: 'query " + params() + "'");
        return;
      }
      
      StringTokenizer st = new StringTokenizer(params, " ");
      Triple.COLLATION index = parseOrder(st.nextToken());
      if (null == index) {
        out.println("illegal collation.");
        return;
      }
      if (!indexes.contains(index)) {
        out.println("index does not exist.");
        return;
      }
      
      // parse the pattern
      if (!st.hasMoreTokens()) {
        out.println("illegal pattern.");
        return;
      }
      String subj = st.nextToken();
      if ("*".equals(subj)) {
        subj = null;
      }
      if (!st.hasMoreTokens()) {
        out.println("illegal pattern.");
        return;
      }
      String pred = st.nextToken();
      if ("*".equals(pred)) {
        pred = null;
      }
      if (!st.hasMoreTokens()) {
        out.println("illegal pattern.");
        return;
      }
      String obj = st.nextToken();
      while (st.hasMoreTokens()) {
        obj += " " + st.nextToken();
      }
      if ("*".equals(obj)) {
        obj = null;
      }
      
      Triple pattern = Triple.newPattern(subj, pred, obj);
      out.println("querying " + pattern + " ...");
      
      // ID doesn't matter since we don't write any triples
      for (Peer peer : peers) {
        peer.setSegmentCatalog(null); // we need to remove the seg catalog from Peer.
      }
      Client client = new Client(-1, conf, indexes, peers); 
      
      TripleScanner scanner = client.getScanner(index, pattern, false);
      
      BufferedReader in = new BufferedReader(new InputStreamReader(Shell.this.in));
      int cnt = 0;
      while (scanner.next()) {
        if (++cnt > 100) {
          out.print("more ... ");
          if (!"".equals(in.readLine())) {
            break;
          }
          cnt = 0;
        }
        out.println(scanner.pop());
      }
      
      scanner.close();
      client.close();
    }
    @Override
    String params() {
      return "<index> <pattern>";
    }
    @Override
    String info() {
      return "Run a query on the HDRS store.";
    }
  }
  
  static String printTriple(Triple t) {
    if (Triple.MAGIC_TRIPLE.equals(t)) {
      return "__low__";
    }
    String str = t.toString();
    if (40 < str.length()) {
      str = str.substring(0, 40) + "...";
    }
    return str;
  }
  
  static String printLargeNumber(long number) {
    if (number > 999000L) {
      return String.format("%4.1fM", number / 1000000.0);
    }
    if (number > 99900L) {
      return String.format("%4dk", number / 1000);
    }
    if (number > 999L) {
      return String.format("%4.1fk", number / 1000.0);
    }
    return String.format("%5d", number);
  }
  
  static Triple.COLLATION parseOrder(String str) {
    try {
      return Triple.COLLATION.valueOf(str.toUpperCase());
    } catch (IllegalArgumentException ex) {
      return null;
    }
  }
  
  

}
