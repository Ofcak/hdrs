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

package de.hpi.fgis.hdrs.node;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.FileImageOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import de.hpi.fgis.hdrs.Configuration;
import de.hpi.fgis.hdrs.LogFormatUtil;
import de.hpi.fgis.hdrs.Peer;
import de.hpi.fgis.hdrs.StoreInfo;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.client.Client;
import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.ipc.MultiSegmentWrite;
import de.hpi.fgis.hdrs.ipc.NodeProtocol;
import de.hpi.fgis.hdrs.ipc.OrderedTriplesWritable;
import de.hpi.fgis.hdrs.ipc.ProtocolVersion;
import de.hpi.fgis.hdrs.ipc.ScanResult;
import de.hpi.fgis.hdrs.ipc.SegmentCatalog;
import de.hpi.fgis.hdrs.ipc.SegmentCatalogUpdates;
import de.hpi.fgis.hdrs.ipc.TransactionState;
import de.hpi.fgis.hdrs.node.Index.SegmentSplit;
import de.hpi.fgis.hdrs.node.SegmentCompactorThread.CompactionRequest;
import de.hpi.fgis.hdrs.routing.PeerView;
import de.hpi.fgis.hdrs.routing.Router;
import de.hpi.fgis.hdrs.routing.SegmentInfo;
import de.hpi.fgis.hdrs.segment.Segment;
import de.hpi.fgis.hdrs.segment.SegmentConfiguration;
import de.hpi.fgis.hdrs.segment.SegmentExport;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.tio.TripleSink;
import de.hpi.fgis.hdrs.tio.TripleSourceWithSize;
import de.hpi.fgis.hdrs.triplefile.FileBlock;
import de.hpi.fgis.hdrs.triplefile.FileImport;
import de.hpi.fgis.hdrs.triplefile.FileStreamer;

public class Node implements Runnable, NodeProtocol, SegmentIdGenerator, SegmentServer {

  static final Log LOG = LogFactory.getLog(Node.class);
  
  
  final Configuration conf;
  
  final SegmentConfiguration segmentConf;
  
  
  /**
   * Main thread running this node.
   */
  Thread mainThread;
  
  Server rpcServer;
  
  
  private final Router router;
  
  
  SegmentFlusherThread flushThread = null;
  
  SegmentCompactorThread compactorThread = null;
  
  SegmentSplitThread splitThread = null;
  
  AntiEntropyThread antiEntropyThread = null;
  
  
  ExecutorService committerPool = null;
  
  
  ExecutorService prefetcherPool = null;

  
  
  volatile boolean online = false;
  
  /**
   * Quit flag
   */
  volatile boolean shutDown = false;
  
  
  /**
   * Kill flag
   */
  volatile boolean kill = false;
  
  
  /**
   * There are up to 6 indexes (SPO, POS, ...).  Note that not all
   * indexes need to be present in the store.
   */
  final Map<Triple.COLLATION, Index> indexes = 
    new EnumMap<Triple.COLLATION, Index>(Triple.COLLATION.class);
  
  
  int segmentIdCounter = 0;
  
  
  /**
   * <p> Segments served by this node that are currently open and online 
   * (readable and writable).  Contains segments for all indexes.
   * 
   * <p> This map is initially empty; segments are loaded on demand.
   */
  final Map<Long, SegmentDescriptor> segments;
  final ReadWriteLock segmentsLock = new ReentrantReadWriteLock();
  
  
  public static enum SegmentStatus {
    OFFLINE, // files closed
    OPEN,   // files open
    ONLINE,  // files open & buffer open
    
    CLOSED, // closed (cannot be opened)
    CLOSE_PENDING,
    SPLIT, // closed because split
    SPLIT_PENDING, // split pending
    
    TRANSFER_IN,
    TRANSFER_OUT;
  }
  
  
  final Map<Long, FileImport> fileImports;
  
  
  /*
   *   LOCK ORDER:
   *   
   *   1. SegmentDescriptor
   *   2. Transaction
   *   3. SegmentDescriptor.transactions
   * 
   */
  
  /**
   * Holds and synchronizes access to runtime information of a segment.
   * (status, transactions, scanners, etc.)
   */
  private class SegmentDescriptor {
    
    volatile SegmentStatus status;
    volatile boolean compacting = false;
    final SegmentInfo info;
    final Index index;
    Segment segment = null;
    final Map<Long, Transaction> transactions;
    
    SegmentDescriptor(SegmentStatus status, SegmentInfo info, Index index) {
      this.status = status;
      this.info = info;
      this.index = index;
      transactions = new HashMap<Long, Transaction>();
    }
    
    /**
     * This MUST NOT be called while holding a Transaction monitor lock!
     * @param takeOnline
     * @return
     * @throws IOException
     */
    synchronized boolean open(boolean takeOnline) throws IOException {
      switch (status) {
      case OFFLINE:
        segment = Segment.openSegment(segmentConf, index, info.getSegmentId());
        segment.setSegmentFlusher(flushThread);
        segment.setSegmentCompactor(compactorThread);
        if (takeOnline) {
          segment.takeOnline();
          status = SegmentStatus.ONLINE;
        } else {
          status = SegmentStatus.OPEN;
        }
        return true;
      case TRANSFER_IN:
        status = SegmentStatus.OPEN;
      case OPEN:
        if (takeOnline) {
          segment.takeOnline();
          status = SegmentStatus.ONLINE;
        }
        return true;
      case ONLINE:
        return true;
      default:
        return false;
      }
    }
    
    // only for OFFLINE -> OPEN, triggered by user (shell).
    synchronized boolean open() throws IOException {
      if (SegmentStatus.OFFLINE == status) {
        return open(false);
      }
      return false;
    }
    
    synchronized boolean close() throws IOException {
      switch (status) {
      case OFFLINE:
        status = SegmentStatus.CLOSED;
        return true;
      case OPEN:
        status = SegmentStatus.CLOSED;
        segment.close();
        return true;
      case ONLINE:
      case CLOSE_PENDING:
      case SPLIT_PENDING:
        if (!compacting && abortAllTransactions()) {
          status = SegmentStatus.CLOSED;
          segment.close();
          return true;
        }
        status = SegmentStatus.CLOSE_PENDING;
        return false;
      case TRANSFER_OUT:
      case TRANSFER_IN:
        return false;
      default: // CLOSED, SPLIT
        return true;
      }
    }
    
    /**
     * This closes the segment, if
     * 1.) it is ONLINE,
     * 2.) it is EMPTY (contains no triples)
     * @return
     */
    synchronized boolean closeIfEmpty() {
      if (!isOnline()) {
        return false;
      }
      synchronized (transactions) {
        if (!(transactions.isEmpty() && segment.closeIfEmpty())) {
          return false;
        }
        status = SegmentStatus.CLOSED;
        return true;
      }
    }
    
    boolean isOnline() {
      return status == SegmentStatus.ONLINE;
    }
    
    boolean isOpen() {
      return status == SegmentStatus.OPEN;
    }
    
    boolean splitPending() {
      return status == SegmentStatus.SPLIT_PENDING;
    }
    
    boolean closePending() {
      return status == SegmentStatus.CLOSE_PENDING;
    }
    
    boolean isAvailable() {
      return SegmentStatus.ONLINE == status
          || SegmentStatus.OPEN == status
          || SegmentStatus.OFFLINE == status;
    }
    
    void addTransaction(Transaction tx) {
      synchronized (transactions) {
        transactions.put(tx.getId(), tx);
      }
    }
    
    void removeTransaction(Transaction tx) {
      synchronized (transactions) {
        transactions.remove(tx.getId());
      }
    }
    
    private boolean abortAllTransactions() {
      // make a copy, otherwise we'll get concurrent mod exceptions...
      List<Transaction> transactions;
      synchronized (this.transactions) {
        transactions = new ArrayList<Transaction>(this.transactions.values());
      }
      // try to abort transactions
      boolean success = true;
      for (Transaction tx : transactions) {
        if (!tx.abort()) {
          success = false;
        }
      }  
      return success;
    }
    
    synchronized boolean shouldBeSplit(boolean scatter) {
      switch (status) {
      case OPEN:
      case ONLINE:
        return segment.shouldBeSplit(scatter);
      }
      return false;
    }
    
    synchronized boolean canBeSplit() {
      switch (status) {
      case SPLIT_PENDING:
        return true;
      case OPEN:
      case ONLINE:
        return segment.canBeSplit();
      }
      return false;
    }
    
    synchronized boolean prepareSplit() throws IOException { 
      switch (status) {
      case OFFLINE:
        open(false);
        status = SegmentStatus.SPLIT;
        return true;
      case OPEN:
        status = SegmentStatus.SPLIT;
        return true;
      case ONLINE:
      case SPLIT_PENDING:
        if (!compacting && abortAllTransactions()) {
          // very well; all transactions dead now.
          status = SegmentStatus.SPLIT;
          return true;
        }
        status = SegmentStatus.SPLIT_PENDING;
        return false;
      case SPLIT:
        throw new IllegalStateException("Segment already split");
      default: // CLOSE, CLOSE_PENDING
        return false;
      }
    }
    
    synchronized void prepareImport() throws IOException {
      if (SegmentStatus.OFFLINE != status) {
        throw new IllegalStateException("Segment transfer not possible in state " + status);
      }
      open(false);
      status = SegmentStatus.TRANSFER_IN;
    }
    
    synchronized void prepareExport() throws IOException {
      if (SegmentStatus.OFFLINE != status) {
        throw new IllegalStateException("Segment transfer not possible in state " + status);
      }
      open(false);
      status = SegmentStatus.TRANSFER_OUT;
    }
    
    synchronized void finishExport() throws IOException {
      if (SegmentStatus.TRANSFER_OUT != status) {
        throw new IllegalStateException("Invalid segment state " + status);
      }
      status = SegmentStatus.OPEN;
      close();
    }
    
    void compact(boolean major) throws IOException {
      synchronized (this) {
        if (!compacting && (SegmentStatus.OPEN == status || SegmentStatus.ONLINE == status)) {
          compacting = true;
        } else {
          LOG.info("Skipping compaction for " + this);
          return;
        }
      }
      LOG.info("Compacting " + this);
      try {
        segment.compact(major);
      } finally {
        compacting = false;
      }
    }
    
    TripleScanner openScanner() throws IOException {
      synchronized (this) {
        if (!open(false)) {
          return null;
        }
      }
      return segment.getScanner();
    }
    
    TripleScanner openScanner(Triple pattern) throws IOException {
      synchronized (this) {
        if (!open(false)) {
          return null;
        }
      }
      return segment.getScanner(pattern);
    }
    
    @Override
    public String toString() {
      return "Segment (id = " + info.getSegmentId() + ", status = "
          + status + ")";
    }
    
  }
  
  
  final Map<Long, Transaction> transactions;
  
  /**
   * Memory pool for transactions.
   */
  final private Semaphore transactionMemPool;
  
  
  /**
   * <p> A write transaction.  A transactions writes triples to a number of segments
   * located on this node.
   * 
   * <p> This transaction class implements the 2-Phase Commit Protocol (2PC).
   * 
   * <p> Triples of a transaction are buffered and only written on commit.
   * 
   * <p> A transaction that is not in 'prepared' state can be aborted anytime by
   * the remote transaction manager or by the node, e.g. when a segment is split.
   * 
   * @author daniel.hefenbrock
   *
   */
  static class Transaction {
    
    static enum State {
      OPEN,
      PREPARED,
      COMMITTED,
      ABORTED;
    }
    
    volatile State state;
    final long id;
    Map<SegmentDescriptor, List<TripleSourceWithSize>> segments;
    
    final Semaphore memPool;
    final int bufferSize;
    int freeBuffer;
    
    public Transaction(long id, Semaphore memPool, int bufferSize) {
      this.state = State.OPEN;
      this.id = id;
      this.segments = new HashMap<SegmentDescriptor, List<TripleSourceWithSize>>();
      this.memPool = memPool;
      this.bufferSize = bufferSize;
      this.freeBuffer = bufferSize;
    }
    
    public long getId() {
      return id;
    }
    
    /**
     * Abort this transaction if possible.  A transaction cannot be aborted if 
     * it is in 'prepared' state.
     * @return
     */
    synchronized boolean abort() {
      if (State.PREPARED == state) {
        return false;
      } else if (State.COMMITTED == state) {
        // tell a lie, it doesn't matter.
        return true;
      }
      return clientAbort();
    }
    
    
    /**
     * Client aborts... the client can abort a transaction even in 'prepared'
     * state!  (not that 'Client' in this case is just another node (aka the
     * transaction manager).
     * @return
     */
    synchronized boolean clientAbort() {
      if (State.COMMITTED == state) {
        return false;
      } else if (State.ABORTED == state) {
        return true;
      }
      state = State.ABORTED;
      // remove this transactions from all segments
      for (SegmentDescriptor sd : segments.keySet()) {
        sd.removeTransaction(this);
      }
      // release resources
      segments = null;
      // release memory from transaction memory pool
      memPool.release(bufferSize);
      return true;   
    }
    
    
    /**
     * <p> The main task of prepare is to make sure that all segments contained
     * in this transaction are writable (online).
     * 
     * <p> ONLY called by RPC handler thread (NOT thread safe).
     * @throws IOException 
     */
    boolean prepare() throws IOException {
      if (State.OPEN == state) {
        Set<SegmentDescriptor> segments;
        synchronized (this) {
          segments = this.segments.keySet();
        }
        if (null == segments) {
          // someone else aborted this transaction.
          return false;
        }
        // pre-check
        for (SegmentDescriptor sd : segments) {
          if (!sd.open(true)) {
            // segment is not writable
            abort();
            return false;
          }
        }
        synchronized (this) {
          // post-check... still not aborted?
          if (State.OPEN == state) {
            // ... segments still online?
            // We MUST do this in this weird way in order to maintain the locking
            // order (1. SegmentDescriptor, 2. Transaction).  Otherwise deadlock.
            for (SegmentDescriptor sd : segments) {
              if (!sd.isOnline()) {
                // segment not writable... abort.
                abort();
                return false;
              }
            }
            // we're good to go.
            state = State.PREPARED;
            return true; // SUCCESS
          }
        }
      }
      if (State.ABORTED == state) {
        // someone else aborted this transaction.
        return false;
      }
      throw new IllegalStateException("cannot prepare transaction in state " + state);
    }
    
    /**
     * 
     * <p> ONLY called by RPC handler thread (NOT thread safe).
     * @throws IOException
     */
    int commit() throws IOException {
      if (State.PREPARED != state) {
        throw new IllegalStateException("cannot commit transaction in state " + state);
      }
      int nBytesAdded = 0;
      for (Map.Entry<SegmentDescriptor, List<TripleSourceWithSize>> entry 
          : segments.entrySet()) {
        SegmentDescriptor sd = entry.getKey();
        if (!sd.isOnline() && !sd.splitPending() && !sd.closePending()) {
          throw new IOException("segment is offline in state 'prepared'");
        }
        for (TripleSourceWithSize triples : entry.getValue()) {
          TripleScanner scanner = triples.getScanner();
          nBytesAdded += sd.segment.batchAdd(scanner);
          if (scanner.next()) {
            throw new IOException("partial write");
          }
          scanner.close();
        }
        sd.removeTransaction(this);
      }
      // we're done.
      state = State.COMMITTED;
      // release memory and resources
      memPool.release(bufferSize);
      segments = null;
      return nBytesAdded;
    }
    
    /**
     * 
     * @throws IOException 
     */
    boolean addTriples(SegmentDescriptor sd, TripleSourceWithSize triples) throws IOException {
      if (State.ABORTED == state) {
        return false;
      }
      freeBuffer -= triples.getSizeBytes();
      if (0 > freeBuffer) {
        // transaction buffer size exceeded!  do abort.
        LOG.info("Transaction buffer size exceeded.  Aborting transaction " + id);
        abort();
        return false;
      }
      synchronized (sd) {
        if (!sd.isAvailable()) {
          // segment was or is about to be split.  cannot be written.
          LOG.info("Transaction aborted.  Segment cannot be written in state " + sd.status);
          abort();
          return false;
        }
        synchronized (this) {
          if (State.OPEN == state) {
            List<TripleSourceWithSize> writes = segments.get(sd);
            if (null == writes) {
              writes = new ArrayList<TripleSourceWithSize>();
              segments.put(sd, writes);
              sd.addTransaction(this);
            }
            writes.add(triples);
            return true;
          } else if (State.ABORTED == state) {
            return false;
          }
          throw new IllegalStateException("cannot add triples to transaction in state " + state);
        } 
      }
    }
    
  }
  
  
  
  
  final File rootDir;
  
  final Set<Triple.COLLATION> orders;
  
  final AtomicLong bufferSize = new AtomicLong(0L);
  
  
  
  Node(Configuration conf, Set<Triple.COLLATION> orders, 
      ArrayList<Peer> peers, Peer localPeer) throws IOException {
    this.conf = conf;
    this.segmentConf = new SegmentConfiguration(conf);
    this.orders = orders;
    this.router = new Router(conf, peers, localPeer, this);
    
    segments = new HashMap<Long, SegmentDescriptor>();
    
    fileImports = new HashMap<Long, FileImport>();
    
    transactions = new HashMap<Long, Transaction>();
    transactionMemPool = new Semaphore(
        conf.getInt(Configuration.KEY_NODE_TRANSACTION_BUFFER, 
            Configuration.DEFAULT_NODE_TRANSACTION_BUFFER));
    
    rootDir = new File(conf.get(Configuration.KEY_ROOT_DIR));
    if (!rootDir.isDirectory()) {
      throw new IOException("hdrs root dir does not exist: " + rootDir);
    }
  }
  
  
  Router getRouter() {
    return router;
  }
  
  
  SegmentCatalog getLocalSegmentCatalog() {
    return router.getLocalCatalog();
  }
  
  
  SegmentDescriptor getSegmentDescriptor(long segmentId) throws IOException {
    segmentsLock.readLock().lock();
    SegmentDescriptor sd = segments.get(segmentId);
    segmentsLock.readLock().unlock();
    if (null == sd) {
      throw new IOException("segment " + segmentId + " not found");
    }
    return sd;
  }
  
  
  @Override
  public boolean splitSegment(long segmentId) throws IOException {
    SegmentDescriptor sd = getSegmentDescriptor(segmentId);
    if (!sd.canBeSplit()) {
      LOG.info("Segment cannot be split because it contains at least one " +
      		"half-file.  Need compaction first.");
      return true; // true = don't try again
    }
    if (!sd.prepareSplit()) {
      // there is at least one transaction in 'prepared' state.  we must
      // wait until it is committed before offlining this segment.
      LOG.info("Postponing split for segment " + sd);
      return false;
    }
    splitSegment(sd);
    return true;
  }
  
  
  /**
   * This assumes sd.prepareSplit() succeeded!
   */
  private void splitSegment(SegmentDescriptor sd) throws IOException {
    
    SegmentSplit split = sd.index.splitSegment(sd.info, sd.segment);
    if (null == split) {
      LOG.warn("couldn't split segment " + sd.info);
      return;
    }
    
    SegmentDescriptor sdTop = 
      new SegmentDescriptor(SegmentStatus.OFFLINE, split.top, sd.index);
    SegmentDescriptor sdBottom = 
      new SegmentDescriptor(SegmentStatus.OFFLINE, split.bottom, sd.index);
    
    segmentsLock.writeLock().lock();
    segments.put(split.top.getSegmentId(), sdTop);
    segments.put(split.bottom.getSegmentId(), sdBottom);
    segmentsLock.writeLock().unlock();
    
    Peer tpeer = router.locateSegment(sd.index.getOrder(), split.top);
    SegmentInfo add1 = null;
    if (router.getLocalPeer() == tpeer) {
      // segment stays at this node
      add1 = split.top;
    } else {
      // export this segment
      startSegmentTransfer(tpeer, sdTop);
    }
    
    Peer bpeer = router.locateSegment(sd.index.getOrder(), split.bottom);
    SegmentInfo add2 = null;
    if (router.getLocalPeer() == bpeer) {
      // segment stays at this node
      add2 = split.bottom;
    } else {
      // export this segment
      startSegmentTransfer(bpeer, sdBottom);
    }
    
    LOG.info("Segment split: " + sd.info + " split into " + 
        sdTop.info + " (Node " + tpeer + ") and " +
        sdBottom.info + " (Node " + bpeer + ")");
    
    // update index.  this is actually the commit point.  if we fail
    // before the split won't be visible but we'll have garbage in
    // the segment directory.
    sd.index.updateSegments(sd.info, add1, add2);
    
    // indicate segment catalog update is needed
    router.localCatalogUpdate();
    
    // remove descriptor
    segmentsLock.writeLock().lock();
    segments.remove(sd.info.getSegmentId());
    segmentsLock.writeLock().unlock();
  }
  
  
  private void startSegmentTransfer(Peer target, SegmentDescriptor sd) throws IOException {
    sd.prepareExport();
    
    SegmentTransferThread transferThread = 
      new SegmentTransferThread(target, sd);
    
    transferThread.start();
  }
  
  
  public static final int FILE_TRANSFER_BATCH_SIZE = 1024 * 1024; // 1 MB
  
  
  private void transferSegment(Peer peer, SegmentDescriptor sd) throws IOException {
    LOG.info("Starting transfer of segment " + sd);
    
    SegmentExport export = sd.segment.export();
    NodeProtocol proxy = peer.getProxy(conf);
      
    long nBytes = 0;
    long time = System.currentTimeMillis();
      
    proxy.startSegmentTransfer(sd.info, sd.index.getOrder().getCode());
    FileStreamer file;
    while (null != (file = export.nextFile())) {
      proxy.startFileTransfer(sd.info.getSegmentId(), file.getCompression().getCode(),
          file.getDataSize(), file.getNumberOfTriples());
      
      while (true) {
        int batchSize = 0;
        List<FileBlock> blocks = new ArrayList<FileBlock>();
        
        FileBlock block;
        while (null != (block = file.nextBlock())) {
          blocks.add(block);
          batchSize += block.size();
          if (FILE_TRANSFER_BATCH_SIZE < batchSize) {
            break;
          }
        }
        if (0 == batchSize) {
          break;
        } 
        proxy.transferFileBlocks(sd.info.getSegmentId(), 
            blocks.toArray(new FileBlock[blocks.size()]));
         
        nBytes += batchSize;
      }
      // copy to make sure its not a ScatteredTriple (RPC can't handle that...)
      proxy.finishFileTransfer(sd.info.getSegmentId(), file.getLastTriple().copy());
    }
    proxy.finishSegmentTransfer(sd.info.getSegmentId());
    RPC.stopProxy(proxy);
    
    time = System.currentTimeMillis() - time;
    LOG.info(String.format("Segment %d transferred to peer %s. (%.2f MB, %.2f MB/s)",
        sd.info.getSegmentId(), peer,
        LogFormatUtil.MB(nBytes),
        LogFormatUtil.MBperSec(nBytes, time)
    ));
    
//  This is for benchmarking segment transfers.
//    System.out.println(String.format("%.2f\t%.2f",
//        time/1000.0,
//        LogFormatUtil.MB(nBytes))
//        );
    
    // transfer is complete.  now get rid of the segment
    sd.finishExport();
    // remove descriptor
    segmentsLock.writeLock().lock();
    segments.remove(sd.info.getSegmentId());
    segmentsLock.writeLock().unlock();
    
    sd.segment.delete();
    
    antiEntropyThread.run(peer);
  }
  
  
  // called by router
  public void runAntiEntropy(Peer peer) {
    antiEntropyThread.run(peer);
  }
  
  
  @Override
  public boolean flushSegment(long segmentId) throws IOException {
    SegmentDescriptor sd = null;
    segmentsLock.readLock().lock();
    sd = segments.get(segmentId);
    segmentsLock.readLock().unlock();
    if (null != sd && (sd.isOnline() || sd.splitPending() || sd.closePending())) {
      LOG.info("Flushing " + sd);
      // force flush if split or close is pending.  otherwise there is possibility of deadlock.
      int nBytesFlushed = sd.segment.flushBuffer(sd.splitPending() || sd.closePending());
      if (0 < nBytesFlushed) {
        // flush succeeded
        bufferSize.addAndGet(-nBytesFlushed);
        return true;
      } else if (0 == nBytesFlushed) {
        // there wasn't anything to flush... fair enough. (but buffer could be closed now)
        return true;
      }
      // too many files!  try again later.
      LOG.info("Flush postponed for " + sd + ".  Too many triple files!");
      return false;
    }
    // don't try again..
    return true;
  }
   
  
  @Override
  public int compactSegment(Set<CompactionRequest> requests) throws IOException {
    CompactionRequest r = null;
    SegmentDescriptor sd = null;
    int removed = 0;
    // LOCK ORDER: 1. segmentsLock, 2. requests.... NOT THE OTHER WAY AROUND!!
    segmentsLock.readLock().lock();
    int maxFiles = 0;
    synchronized (requests) {
      Iterator<CompactionRequest> it = requests.iterator();
      while (it.hasNext()) {
        CompactionRequest request = it.next();
        SegmentDescriptor rsd = segments.get(request.getSegmentId());
        if (null == rsd || !rsd.isAvailable() || null == rsd.segment) {
          it.remove();
          removed++;
          continue;
        }
        if (rsd.segment.getNumberOfFiles() > maxFiles) {
          r = request;
          sd = rsd;
          maxFiles = rsd.segment.getNumberOfFiles();
        }
      }
      if (null == r) {
        segmentsLock.readLock().unlock();
        return removed;
      }
      requests.remove(r);
      removed++;
    }
    segmentsLock.readLock().unlock();
    sd.compact(r.isMajor());
    // check split.. should we split it?
    if (sd.shouldBeSplit(scatterSegments())) {
      // yes, can we split it right now?
      if (sd.prepareSplit()) {
        // yep, do it.
        splitSegment(sd);
      } else {
        // status is now SPLIT_PENDING
        // try splitting it later
        splitThread.request(r.getSegmentId(), SegmentSplitThread.SPLIT_RETRY_DELAY);
        LOG.info("Postponing split for segment " + sd);
      }
    // segment could be empty now.  in this case, close and
    // remove the segment if it is not the root segment (lo triple not magic)
    } else if (!sd.info.getLowTriple().isMagic()
        && sd.closeIfEmpty()) {
      LOG.info("segment " + sd + " became empty after compaction ... removing it");
      // segment is closed now.  remove it
      sd.index.updateSegments(sd.info, null, null);
      // indicate segment catalog update is needed
      router.localCatalogUpdate();
      // remove descriptor
      segmentsLock.writeLock().lock();
      segments.remove(sd.info.getSegmentId());
      segmentsLock.writeLock().unlock();
      // delete segment from disk
      Segment.deleteSegment(sd.index, sd.info.getSegmentId());
    }
    return removed;
  }
  
  
  // when the store is still small, we want to increase the number
  // of segments fast in order to leverage all nodes
  private boolean scatterSegments() {
    return router.getNumberOfSegments() < router.getPeers().size()
        * conf.getFloat(Configuration.KEY_SCATTER_FACTOR, Configuration.DEFAULT_SCATTER_FACTOR);
  }
  
  
  @Override
  public synchronized long generateSegmentId() {
    return (((long) router.getLocalPeer().getId()) << 31) + this.segmentIdCounter++;
  }
  
  
  private static final String SEGMENT_ID_COUNTER_FILE = "storemeta";
  
  private void writeSegmentIdCounter() {
    try {
      File f = new File(rootDir.getAbsolutePath() + File.separator + SEGMENT_ID_COUNTER_FILE);
      if (f.isFile()) {
        if (!f.delete()) {
          throw new IOException("Cannot delete old index meta file");
        }
      }
      FileImageOutputStream out = new FileImageOutputStream(f);
      out.writeInt(segmentIdCounter);
      out.close();
    } catch (IOException ex) {
      LOG.error("Error writing store meta file", ex);
    }
  }
  
  
  boolean initialize() {
    // read segment id counter
    File storemeta = new File(rootDir.getAbsolutePath() + File.separator + SEGMENT_ID_COUNTER_FILE);
    if (storemeta.isFile()) {
      FileImageInputStream in;
      try {
        in = new FileImageInputStream(storemeta);
        segmentIdCounter = in.readInt();
        in.close();
      } catch (IOException ex) {
        LOG.fatal("error while reading store meta file ", ex);
        return false;
      }
    }
    
    LOG.info("Using segment configuration: " + segmentConf);
    
    // init indexes
    for (Triple.COLLATION order : orders) {
      try {
        File indexRoot = new File(rootDir.getAbsolutePath() + File.separator + order);
      
        Index index;
        if (indexRoot.isDirectory()) {
          LOG.info("Opening index " + order);
          index = Index.openIndex(indexRoot, order, this);
        } else {
          LOG.info("Creating index " + order);
          index = Index.createIndex(indexRoot, order, this);
          if (router.getLocalPeer() == router.locateSegment(order, SegmentInfo.getSeed(0))) {
            index.createSeedSegment();
          }
        }
      
        indexes.put(order, index);
        router.initialize(order);
    
      } catch (IOException ex) {
        LOG.fatal("error while opening index " + order, ex);
        return false;
      }
    }
    
    router.syncLocalCatalog();
    
    // create segment descriptors
    for (Index index : indexes.values()) {
      for (SegmentInfo info : router.getLocalCatalog().getSegments(index.getOrder())) {
        segments.put(Long.valueOf(info.getSegmentId()), 
            new SegmentDescriptor(SegmentStatus.OFFLINE, info, index));
      }
    }
    
    LOG.info("Segment catalog initialized: " + router.getLocalCatalog());
    return true;
  }
  
  
  void startThreads() {
    flushThread = new SegmentFlusherThread(this);
    compactorThread = new SegmentCompactorThread(this);
    splitThread = new SegmentSplitThread(this);
    antiEntropyThread = new AntiEntropyThread(router);
    flushThread.start();
    compactorThread.start();
    splitThread.start();
    antiEntropyThread.start();
    
    committerPool = Executors.newFixedThreadPool(
        conf.getInt(Configuration.KEY_COMMIT_POOL_SIZE, 
            Configuration.DEFAULT_COMMIT_POOL_SIZE));
    prefetcherPool = Executors.newFixedThreadPool(
        conf.getInt(Configuration.KEY_PREFTECH_POOL_SIZE, 
            Configuration.DEFAULT_PREFETCH_POOL_SIZE));
  }
  
  
  void stopThreads() {
    
    committerPool.shutdown();
    try {
      while (!committerPool.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.info("Waiting for committer thread pool to shutdown...");
      }
    } catch (InterruptedException e) {
      LOG.warn("Committer thread pool didn't shutdown properly.");
    }
    prefetcherPool.shutdown();
    try {
      while (!prefetcherPool.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.info("Waiting for prefetcher thread pool to shutdown...");
      }
    } catch (InterruptedException e) {
      LOG.warn("Prefetcher thread pool didn't shutdown properly.");
    }
    
    if (null != flushThread) {
      flushThread.quitJoin();
    }
    if (null != compactorThread) {
      compactorThread.quitJoin();
    }
    if (null != splitThread) {
      splitThread.quitJoin();
    }
    if (null != antiEntropyThread) {
      antiEntropyThread.quitJoin();
    }
  }
  
  
  void close() {
    for (SegmentDescriptor sd : segments.values()) {
      try {
        while (!sd.close()) {
          LOG.info("Waiting for " + sd + " to close...");
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            LOG.warn(sd + " wasn't closed properly.");
          }
        }
      } catch (IOException ex) {
        LOG.error("Error while trying to close segment ", ex);
      }
    }
  }
  
  // for test clean ups
  public void delete() throws IOException {
    for (Index index : indexes.values()) {
      index.delete();
    }
  }
  
  // for testing
  public Client getClient(Configuration conf) throws IOException {
    // we MUST make a copy since peers store routing
    // information 
    ArrayList<Peer> peers = new ArrayList<Peer>();
    for (Peer peer : router.getPeers()) {
      peers.add(new Peer(peer));
    }
    return new Client(1, conf, orders, peers);
  }
  
  
  @Override
  public synchronized void stop(Throwable cause) {
    if (cause instanceof OutOfMemoryError) {
      LOG.error("Segment buffer size: " + bufferSize.get());
    }
    if (shutDown) {
      return;
    }
    shutDown = true;
    mainThread.interrupt();
  }
  

  @Override
  public void run() {
    mainThread = Thread.currentThread();
    LOG.info("Node " + router.getLocalPeer().toString() + " starting up...");
    
    startThreads();
    if (!initialize()) {
      return;
    }
    
    // start rpc server
    try {
      /*   HADOOP 0.21.0:
      rpcServer = RPC.getServer(NodeProtocol.class, this, 
          router.getLocalPeer().getAddress(), 
          router.getLocalPeer().getPort(),
          conf.getInt(Configuration.KEY_RPC_HANDLER_COUNT, Configuration.DEFAULT_RPC_HANDLER_COUNT),
          false, conf, null);*/
      rpcServer = RPC.getServer(this, 
          router.getLocalPeer().getAddress(), 
          router.getLocalPeer().getPort(),
          conf.getInt(Configuration.KEY_RPC_HANDLER_COUNT, Configuration.DEFAULT_RPC_HANDLER_COUNT),
          false, conf);
      rpcServer.start();
    } catch (IOException ex) {
      LOG.fatal("Error during rpc server creation." , ex);
      return;
    }
    
    // now we're going online
    online = true;
    
    // main loop
    while (!shutDown) {
      try {
        Thread.sleep(20 * 1000);
        //System.out.println("Buffer: " + LogFormatUtil.MB(bufferSize.get()));
      } catch (InterruptedException ex) {
        // ignore
      }
    }
    
    LOG.info("Node " + router.getLocalPeer().toString() + " shutting down...");
    online = false;
    
    stopThreads();
    if (!kill) {
      close();
    }
    
    writeSegmentIdCounter();
    
    // wait 1 sec to allow stopper to disconnect
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      // ignore
    }
    rpcServer.stop();
    try {
      rpcServer.join();
    } catch (InterruptedException ex) {
      // ignore
    }
  }



  @Override
  public void shutDown() {
    LOG.info("Triggering node shut down for node " + router.getLocalPeer().toString());
    shutDown = true;
    mainThread.interrupt();
  }
  
  
  @Override
  public void kill() {
    LOG.info("Killing node " + router.getLocalPeer().toString());
    kill = true;
    shutDown = true;
    mainThread.interrupt();
  }
  
  
  // for testing to wait until the node is online.
  public void waitOnline() {
    while (!online) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        return;
      }
    }
  }
  
  // for testing
  public void waitDone() {
    try {
      mainThread.join();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for node main thread to finish");
    }
  }


  // NOT NEEDED IN HADOOP 0.21.0
  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return ProtocolVersion.versionID;
  }
  
   
  public SegmentCatalog getLocalSegmentCatalog(long version) {
    Map<Triple.COLLATION, List<SegmentInfo>> lists = 
      new EnumMap<Triple.COLLATION, List<SegmentInfo>>(Triple.COLLATION.class);
    for (Index index : indexes.values()) {
      lists.put(index.getOrder(), index.getSegments());
    }
    return new SegmentCatalog(version, lists);
  }
  
  
  @Override
  public SegmentCatalogUpdates antiEntropy(long[] versionVector) throws IOException {
    //LOG.info("Received anti-entropy request from " + Server.getRemoteAddress());    
    return router.antiEntropy(versionVector);
  }
  
  
  
  Transaction getTransaction(long transactionId) throws IOException {
    synchronized (transactions) {
      Transaction tx = transactions.get(Long.valueOf(transactionId));
      if (null == tx) {
        throw new IOException("unknown transaction id " + transactionId);
      }
      return tx;
    }
  }
  
  boolean removeTransaction(long transactionId) {
    synchronized (transactions) {
      return null != transactions.remove(Long.valueOf(transactionId));
    }
  }
  

  class CommitTask implements Runnable {
    private final Transaction transaction;
    CommitTask(Transaction transaction) {
      this.transaction = transaction;
    }
    @Override
    public void run() {
      int nBytesAdded;
      try {
        nBytesAdded = transaction.commit();
        removeTransaction(transaction.getId());
        bufferSize.addAndGet(nBytesAdded);
      } catch (Throwable e) {
        LOG.fatal("error while committing transaction" + transaction.getId(), e);
        stop(e);
      }
    }
  }
  

  @Override
  public void commitTransaction(long transactionId) throws IOException {
    Transaction tx = getTransaction(transactionId);
    CommitTask commitTask = new CommitTask(tx);
    if (null == committerPool) {
      // looks like we have to do it ourselves...  (might be the case during testing)
      commitTask.run();
    } else {
      try {
        committerPool.execute(commitTask);
      } catch (RejectedExecutionException ex) {
        LOG.warn("Commit rejected by thread pool... running blocking commit.");
        commitTask.run();
      }
    }
  }


  @Override
  public TransactionState prepareTransaction(long transactionId) throws IOException {
    Transaction tx = getTransaction(transactionId);
    if (!tx.prepare()) {
      // transaction failed
      removeTransaction(transactionId);
      return TransactionState.ABORTED;
    }
    return TransactionState.OK;
  }


  @Override
  public BooleanWritable startTransaction(long transactionId, int bufferSize, long timeout) 
  throws IOException {
    // can we allocate memory from the transaction pool?
    try {
      if (!transactionMemPool.tryAcquire(bufferSize, timeout, TimeUnit.MILLISECONDS)) {
        // nope...
        LOG.info("Unable to allocate " + LogFormatUtil.MB(bufferSize) 
            + " MB for transaction " + transactionId);
        return FALSE;
      }
    } catch (InterruptedException e) {
      return FALSE;
    }
    Transaction tx = new Transaction(transactionId, transactionMemPool, bufferSize);
    synchronized (transactions) {
      if (null != transactions.put(Long.valueOf(transactionId), tx)) {
        transactionMemPool.release(bufferSize);
        throw new IOException("transaction id collision");
      }
    }
    return TRUE;
  }


  @Override
  public TransactionState writeTriples(long transactionId, MultiSegmentWrite writes) 
  throws IOException {
    
    Transaction tx = getTransaction(transactionId);
    
    for (long segmentId : writes.getSegments()) {
      
      SegmentDescriptor sd = null;
      segmentsLock.readLock().lock();
      sd = segments.get(segmentId);
      segmentsLock.readLock().unlock();
      if (null == sd) {
        LOG.info("Segment " + segmentId + " not found.  Aborting transaction");
        // segment not found!
        tx.abort();
        removeTransaction(transactionId);
        return TransactionState.aborted(segmentId);
      }
      
      if (!tx.addTriples(sd, writes.getTriples(segmentId))) {
        // transaction failed
        removeTransaction(transactionId);
        return TransactionState.aborted(segmentId);
      }
      
    }
    
    return TransactionState.OK;
  }
  
  
  @Override
  public TransactionState writeAndPrepare(long transactionId, MultiSegmentWrite writes)
      throws IOException {
    TransactionState writeState = writeTriples(transactionId, writes);
    if (writeState.isOk()) {
      return prepareTransaction(transactionId);
    }
    return writeState;
  }
  
  
  @Override
  public void abortTransaction(long transactionId) throws IOException {
    synchronized (transactions) {
      Transaction tx = transactions.get(Long.valueOf(transactionId));
      if (null != tx && !tx.clientAbort()) {
        throw new IOException("Cannot abort transaction in state COMMITTED");
      }
    }
    removeTransaction(transactionId);
  }


  @Override
  public boolean finishSegmentTransfer(long segmentId) throws IOException {
    SegmentDescriptor sd = getSegmentDescriptor(segmentId);
    sd.index.updateSegments(null, sd.info, null);
    
    LOG.info("Segment transfer-in successful (" + sd + ")");
    
    sd.open(false);
    
    // indicate segment catalog update is needed
    router.localCatalogUpdate();
    
    return true;
  }


  @Override
  public void finishFileTransfer(long segmentId, Triple lastTriple) throws IOException {
    FileImport fileImport = getFileImport(segmentId);
    if (fileImport.close(lastTriple)) {
      //LOG.info("File transfer finished");
    } else {
      //LOG.info("File transfer finished with no blocks (file deleted)");
    }
    synchronized (fileImports) {
      fileImports.remove(segmentId);
    }
  }


  @Override
  public void startFileTransfer(long segmentId, byte compressionAlgorithm,
      long dataSize, long nTriples) throws IOException {
    
    //LOG.info("Starting file transfer-in for segment " + segmentId);
    
    SegmentDescriptor sd = getSegmentDescriptor(segmentId);
    if (sd.status != SegmentStatus.TRANSFER_IN) {
      throw new IOException("Cannot import file in state " + sd.status);
    }
    FileImport fileImport = sd.segment.fileImport(
        Compression.ALGORITHM.decode(compressionAlgorithm), dataSize, nTriples);
    synchronized (fileImports) {
      if (null != fileImports.put(Long.valueOf(segmentId), fileImport)) {
        throw new IOException("Cannot import more than one file at a time.");
      }
    }
  }


  @Override
  public void startSegmentTransfer(SegmentInfo segment, byte collation) throws IOException {
    
    LOG.info("Starting transfer-in of segment " + segment);
    
    Triple.COLLATION order = Triple.COLLATION.decode(collation);
    
    Index index = indexes.get(order);
    index.createSegment(segment.getSegmentId());
   
    SegmentDescriptor sd;
    segmentsLock.writeLock().lock();
    try {
      sd = new SegmentDescriptor(SegmentStatus.OFFLINE, segment, index);
      if (null != segments.put(segment.getSegmentId(), sd)) {
        throw new IOException("Segment id collision for segment " + segment.getSegmentId());
      }
    } finally {
      segmentsLock.writeLock().unlock();
    }
    sd.prepareImport();
  }


  @Override
  public void transferFileBlocks(long segmentId, FileBlock blocks[]) throws IOException {
    FileImport fileImport = getFileImport(segmentId);
    fileImport.addBlocks(blocks);
    //LOG.info("Received " + blocks.length + " file blocks");
  }

  private FileImport getFileImport(long segmentId) throws IOException {
    FileImport fileImport;
    synchronized (fileImports) {
      fileImport = fileImports.get(segmentId);
    }
    if (null == fileImport) {
      throw new IOException("Cannot find file import for segment " + segmentId);
    }
    return fileImport;
  }
  
  
  
  
  
  private class SegmentTransferThread extends Thread {
    
    final Peer peer;
    final SegmentDescriptor sd;
    
    SegmentTransferThread(Peer peer, SegmentDescriptor sd) {
      this.peer = peer;
      this.sd = sd;
    }
    
    @Override 
    public void run() {
      try {
        transferSegment(peer, sd);
      } catch (Throwable ex) {
        LOG.fatal("Segment transfer failed: " + sd, ex);
        Node.this.stop(ex);
      }
    }
    
  }



  /*
   * Node Management
   */

  @Override
  public SegmentSummary[] getIndexSummary(byte collation) throws IOException {
    Triple.COLLATION order = Triple.COLLATION.decode(collation);
    SegmentInfo[] index = router.getIndex(order);
    if (null == index) {
      return null;
    }
    SegmentSummary[] summary = new SegmentSummary[index.length];
    for (int i=0; i<index.length; ++i) {
      summary[i] = new SegmentSummary(
          index[i], router.locateSegment(order, index[i]).getId(), order);
    }
    return summary;
  }
  
  
  @Override
  public SegmentSummary[] getPeerSummary() {
    segmentsLock.readLock().lock();
    try {
      ArrayList<SegmentSummary> segments = new ArrayList<SegmentSummary>();
      for (SegmentDescriptor sd : this.segments.values()) {
        Segment segment = sd.segment;
        segments.add(new SegmentSummary(
            sd.info, sd.status, 
            null==segment ? null : segment.getSegmentSize(),
            router.getLocalPeer().getId(),
            sd.index.getOrder(),
            sd.compacting, null));
      }
      return segments.toArray(new SegmentSummary[segments.size()]);
    } finally {
      segmentsLock.readLock().unlock();
    }
  }


  @Override
  public SegmentSummary getSegmentSummary(long segmentId) throws IOException {
    SegmentDescriptor sd = null;
    segmentsLock.readLock().lock();
    sd = segments.get(segmentId);
    segmentsLock.readLock().unlock();
    if (null == sd) {
      return null;
    }
    sd.open();
    Segment segment = sd.segment;
    if (null == segment) {
      return null;
    }
    return new SegmentSummary(
        sd.info, sd.status, 
        segment.getSegmentSize(),
        router.getLocalPeer().getId(),
        sd.index.getOrder(),
        sd.compacting,
        segment.getFileInfo());
  }
  
  
  @Override
  public PeerView[] getView() {
    return router.getView();
  }


  @Override
  public boolean openSegment(long segmentId) throws IOException {
    if (0 == segmentId) {
      segmentsLock.readLock().lock();
      try {
        for (SegmentDescriptor sd : segments.values()) {
          sd.open();
        }
      } finally {
        segmentsLock.readLock().unlock();
      }
      return true;
    } else {
      SegmentDescriptor sd = null;
      segmentsLock.readLock().lock();
      sd = segments.get(segmentId);
      segmentsLock.readLock().unlock();
      if (null == sd) {
        return false;
      }
      return sd.open();
    }
  }
  
  
  @Override
  public boolean requestCompaction(long segmentId) throws IOException {
    if (0 == segmentId) {
      segmentsLock.readLock().lock();
      try {
        for (SegmentDescriptor sd : segments.values()) {
          sd.open();
          compactorThread.request(sd.info.getSegmentId(), true);
        }
      } finally {
        segmentsLock.readLock().unlock();
      }
      return true;
    } else {
      SegmentDescriptor sd = null;
      segmentsLock.readLock().lock();
      sd = segments.get(segmentId);
      segmentsLock.readLock().unlock();
      if (null == sd) {
        return false;
      }
      sd.open();
      sd.segment.requestMajorCompaction();
      return true;
    }
  }
  
  
  @Override
  public boolean requestFlush(long segmentId) throws IOException {
    if (0 == segmentId) {
      segmentsLock.readLock().lock();
      try {
        for (SegmentDescriptor sd : segments.values()) {
          sd.open();
          flushThread.request(sd.info.getSegmentId());
        }
      } finally {
        segmentsLock.readLock().unlock();
      }
      return true;
    } else {
      SegmentDescriptor sd = null;
      segmentsLock.readLock().lock();
      sd = segments.get(segmentId);
      segmentsLock.readLock().unlock();
      if (null == sd) {
        return false;
      }
      sd.open();
      flushThread.request(sd.info.getSegmentId());
      return true;
    }
  }
  
  
  @Override
  public boolean requestSplit(long segmentId) throws IOException {
    SegmentDescriptor sd = null;
    segmentsLock.readLock().lock();
    sd = segments.get(segmentId);
    segmentsLock.readLock().unlock();
    if (null == sd) {
      return false;
    }
    sd.open();
    splitThread.request(sd.info.getSegmentId());
    return true;
  }

  
  @Override
  public NodeStatus getNodeStatus() {
    Runtime rt = Runtime.getRuntime();
    return new NodeStatus(
        rt.freeMemory() + (rt.maxMemory() - rt.totalMemory()),
        bufferSize.get(),
        conf.getInt(Configuration.KEY_NODE_TRANSACTION_BUFFER, 
            Configuration.DEFAULT_NODE_TRANSACTION_BUFFER) 
            - transactionMemPool.availablePermits(),
        segments.size(),
        transactions.size(),
        scanners.size());
  }

  
  /*
   * 
   *  Reading / Scanning
   * 
   */
  
  public static final int SCAN_BATCH_SIZE = 1024 * 1024;
  
  private final AtomicLong scannerIdCounter = new AtomicLong(0L);
  
  private final Map<Long, SegmentScanner> scanners = new HashMap<Long, SegmentScanner>();
  
  
  private long createScannerId() {
    return scannerIdCounter.incrementAndGet();
  }
  
  
  static class SegmentScanner {
    
    private final long id;
    private final SegmentDescriptor sd;
    private final TripleScanner scanner;
    private final Triple pattern;
    
    private Future<ScanResult> prefetch = null;
    
    private final boolean filterDeletes;
    
    SegmentScanner(long id, SegmentDescriptor sd, TripleScanner scanner, 
        boolean filterDeletes) {
      this(id, sd, scanner, filterDeletes, null);
    }
    
    SegmentScanner(long id, SegmentDescriptor sd, TripleScanner scanner,
        boolean filterDeletes, Triple pattern) {
      this.id = id;
      this.sd = sd;
      this.scanner = scanner;
      this.filterDeletes = filterDeletes;
      this.pattern = pattern;
    }

    Long getId() {
      return id;
    }
    
    ScanResult next(int limit) throws IOException {
      return next(limit, false);
    }
    
    ScanResult next(int limit, boolean isPrefetch) throws IOException {
      
      if (!isPrefetch && null != prefetch) {
        try {
          ScanResult result = prefetch.get();
          prefetch = null;
          return result;
        } catch (InterruptedException e) {
          throw new IOException("interrupted", e);
        } catch (ExecutionException e) {
          throw new IOException("prefetch error", e);
        }
      }
      
      OrderedTriplesWritable triples = new OrderedTriplesWritable(
          sd.index.getOrder(), SCAN_BATCH_SIZE);
      ScanResult result = new ScanResult(id, triples);
      
      TripleSink out = triples.getWriter();
      
      int count = 0;
      while (true) {
        
        if (!scanner.next()) {
          if (!sd.isOnline() && !sd.isOpen()) {
            result.setAborted();
          } else { 
            scanner.close();
            result.setEnd();
          }
          break;
        }
        
        Triple t = scanner.peek();
        
        if (null != pattern
            && !sd.index.getOrder().comparator().match(t, pattern)) {
          scanner.close();
          result.setDone();
          break;
        }
        
        if (filterDeletes) {
          if (t.isDelete()) {
            continue;
          }
        }
        
        if (!out.add(t)) {
          // buffer is full
          if (0 == count) {
            // large triple
            return new ScanResult(id, scanner.pop(), sd.index.getOrder());
          }
          break;
        }
        scanner.pop();
        
        count++;
        if (0 < limit && count == limit) {
          scanner.close();
          result.setAborted();
          break;
        }
      }
      
      return result;
    }
    
    void setPrefetch(Future<ScanResult> prefetch) {
      this.prefetch = prefetch;
    }
    
    void close() throws IOException {
      scanner.close();
    }
    
  }
  
  static class PrefetchTask implements Callable<ScanResult> {
    private final SegmentScanner scanner;
    PrefetchTask(SegmentScanner scanner) {
      this.scanner = scanner;
    }
    @Override
    public ScanResult call() throws Exception {
      return scanner.next(0, true);
    }
  }
  
  
  void prefetch(SegmentScanner scanner) {
    PrefetchTask task = new PrefetchTask(scanner);
    try {
      Future<ScanResult> prefetch = prefetcherPool.submit(task);
      scanner.setPrefetch(prefetch);
    } catch (RejectedExecutionException ex) {
      // skip prefetch in this case
      LOG.warn("Prefetch rejected");
    }
  }
  
  
  SegmentScanner getScanner(long scannerId) throws IOException {
    SegmentScanner scanner;
    synchronized (scanners) {
      scanner = scanners.get(Long.valueOf(scannerId));
    }
    if (null == scanner) {
      throw new IOException("Unknown scanner");
    }
    return scanner;
  }
  
  
  @Override
  public void close(long scannerId) throws IOException {
    SegmentScanner scanner = getScanner(scannerId);
    scanner.close();
    synchronized (scanners) {
      scanners.remove(scanner.getId());
    }
  }


  @Override
  public ScanResult next(long scannerId, int limit) throws IOException {
    SegmentScanner scanner = getScanner(scannerId);
    ScanResult result = scanner.next(limit);
    if (result.isClosed()) {
      synchronized (scanners) {
        scanners.remove(scanner.getId());
      }
    } else {
      prefetch(scanner);
    }
    return result;
  }


  @Override
  public ScanResult openScanner(long segmentId, int limit) throws IOException {
    return openScanner(segmentId, true, limit, null);
  }
  
  
  @Override
  public ScanResult openScanner(long segmentId, boolean filterDeletes, int limit) 
  throws IOException {
    return openScanner(segmentId, filterDeletes, limit, null);
  }


  @Override
  public ScanResult openScanner(long segmentId, boolean filterDeletes, int limit, 
      Triple pattern)
  throws IOException {
    return openScanner(segmentId, filterDeletes, limit, pattern, pattern);
  }
  
  
  @Override
  public ScanResult openScanner(long segmentId, boolean filterDeletes, int limit, 
      Triple pattern, Triple seek) 
  throws IOException {
    SegmentDescriptor sd = null;
    segmentsLock.readLock().lock();
    sd = segments.get(segmentId);
    segmentsLock.readLock().unlock();
    
    if (null == sd) {
      return ScanResult.INVALID;
    }
    
    if (null != seek && seek.isMagic()) {
      // Segment can't handle the magic triple
      seek = null;
    }
    
    TripleScanner tripleScanner = null == seek ? sd.openScanner() : sd.openScanner(seek);
    if (null == tripleScanner) {
      return ScanResult.EMPTY;
    }
    
    SegmentScanner scanner = new SegmentScanner(createScannerId(), 
        sd, tripleScanner, filterDeletes, pattern);
    ScanResult result = scanner.next(limit);
    
    if (!result.isClosed()) {
      synchronized (scanners) {
        scanners.put(scanner.getId(), scanner);
      }
    } else {
      prefetch(scanner);
    }
    return result;
  }
  
  
  
  private int clientIdCounter = 0;
  
  @Override
  public synchronized StoreInfo getStoreInfo() {
    return new StoreInfo(router.getCollations(), router.getPeers(), clientIdCounter++);
  }


  


  


  

  
  
  
}
