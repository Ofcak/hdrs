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

package de.hpi.fgis.hdrs.client;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;

import de.hpi.fgis.hdrs.Configuration;
import de.hpi.fgis.hdrs.Peer;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.ipc.MultiSegmentWrite;
import de.hpi.fgis.hdrs.ipc.NodeProtocol;
import de.hpi.fgis.hdrs.ipc.TransactionState;
import de.hpi.fgis.hdrs.routing.Router;
import de.hpi.fgis.hdrs.routing.SegmentInfo;
import de.hpi.fgis.hdrs.tio.MultiSourceScanner;
import de.hpi.fgis.hdrs.tio.ScannerStack;
import de.hpi.fgis.hdrs.tio.TripleSink;

/**
 * This class is used by client applications to write 
 * triples into a HDRS store.
 * @author hefenbrock
 * 
 */
public class TripleOutputStream implements TripleSink, Flushable, Closeable {

  static final Log LOG = LogFactory.getLog(TripleOutputStream.class);
  
  private final static Method startTransactions;
  private final static Method writeAndPrepare;
  private final static Method commit;
  private final static Method abortTransaction;
  
  
  static {
    try {
      startTransactions = NodeProtocol.class.getDeclaredMethod(
          "startTransaction", long.class, int.class, long.class);
      writeAndPrepare = NodeProtocol.class.getDeclaredMethod(
          "writeAndPrepare", long.class, MultiSegmentWrite.class);
      commit = NodeProtocol.class.getDeclaredMethod(
          "commitTransaction", long.class);
      abortTransaction = NodeProtocol.class.getDeclaredMethod(
          "abortTransaction", long.class);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }
  
  
  interface TransactionIdGenerator {
    
    long nextTransactionId();
    
  }

  private final int transaction_size_node;
  private final int transaction_size;
  
  
  private final Router router;
  
  private final Set<Triple.COLLATION> collations;
  
  
  private final TransactionIdGenerator txIdGen;
  private long transactionId;
  
  private SegmentInfo abortedSegment = null;
  
  private Map<SegmentInfo, MultiSegmentWrite> segments;
  
  private SortedMap<Peer, MultiSegmentWrite> peers;
  
  private int totalSize = 0;
  
  // stats
  private long timeStalled = 0;
  private long timeRouterUpdate = 0;
  private long abortedTransactions = 0;
  
  
  TripleOutputStream(Router router, TransactionIdGenerator gen, 
      Set<Triple.COLLATION> collations) {
    this.router = router;
    this.txIdGen = gen;
    this.collations = collations;
    transaction_size_node = router.getConf().getInt(
        Configuration.KEY_TRANSACTION_SIZE_NODE, Configuration.DEFAULT_TRANSACTION_SIZE_NODE);
    transaction_size = router.getConf().getInt(
        Configuration.KEY_TRANSACTION_SIZE, Configuration.DEFAULT_TRANSACTION_SIZE);
    clear();
  }
  
  
  private void clear() {
    transactionId = txIdGen.nextTransactionId();
    totalSize = 0;
    segments = new HashMap<SegmentInfo, MultiSegmentWrite>();
    peers = new TreeMap<Peer, MultiSegmentWrite>();
  }
  
  
  @Override
  public boolean add(Triple t) throws IOException {
    if (addInternal(t)) {
      flush();
    }
    return true;
  }
  
  
  // returns true if we need to flush.
  private boolean addInternal(Triple t) throws IOException {

    boolean flush = false;
    
    // add triples to all orders that are present in the store
    for (Triple.COLLATION order : collations) {
      // get the segment for this triple
      SegmentInfo segment = router.locateTriple(order, t);
      
      MultiSegmentWrite writes = segments.get(segment);
      if (null == writes) {
        // no writes so far for this segment
        // now check, are there writes for the node?
        Peer peer = router.locateSegment(order, segment);
        writes = peers.get(peer);
        if (null == writes) {
          // nope, add it
          writes = new MultiSegmentWrite();
          // add the peer to this transaction
          peers.put(peer, writes);
        }
        // add the segment to this transaction
        segments.put(segment, writes);
      }
      
      // now add triple
      if (writes.add(segment.getSegmentId(), order, t)) {
        totalSize += t.estimateSize();
      }
      
      // sanity check
//      if (0 >= order.magicComparator().compare(segment.getHighTriple(), t)
//          && !segment.getHighTriple().isMagic()) {
//        throw new IOException("triple larger than low triple");
//      }
      
      // check if the transaction size for this peer is exceeded
      if (writes.getSizeBytes() > transaction_size_node) {
        flush = true;
      }
    }
    
    // note that we count the triple only once, not once per order.
//    totalSize += t.estimateSize();
    
    // do we need to flush? 
    return flush || totalSize > transaction_size;
  }
  
  
  /**
   * <p>Causes the output stream to write buffered triples
   * to the HDRS store. 
   * <p>Note this is automatically done once the buffer 
   * limit is reached.
   */
  public void flush() throws IOException {
    
    int tries = 1;
    
    // this scanner stack stores triples left from aborted flushes
    ScannerStack remainingTriples = null;
    
    do {
    
      if (null != remainingTriples) {
        while (remainingTriples.next()) {
          if (addInternal(remainingTriples.pop())) {
            // run these transactions...
            break;
          }
        }
      }
      
      if (!tryFlush()) {
        // we have a problem.
        LOG.info("Tranaction failed (try #" + tries++ + ")" +
            (null != abortedSegment ? " couldn't write " + abortedSegment : ""));
     
        abortedTransactions++;
        long time = System.currentTimeMillis();
        
        // there is probably a segment transfer in progress.
        // we need to back off for a sec
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // ignore
        }
        
        // update routing info
        updateRouter();
        
        timeRouterUpdate += System.currentTimeMillis() - time;
      
        // we need to re-insert everything.  the problem is that with a 
        // changed segment "topology", there is a possibility that we 
        // cannot run everything in one run.
        // this can also happen several times in a row.
        if (null == remainingTriples) {
          remainingTriples = new ScannerStack();
        }
        remainingTriples.push(new MultiSourceScanner(peers.values()));
        
        clear();
      }
      
    } while (null != remainingTriples && remainingTriples.next());
    
  }
  
  
  /**
   * Close this output stream.  It is important to call
   * this, otherwise triples written to the stream might
   * be lost.
   */
  @Override
  public void close() throws IOException {
    flush();
  }
  
  
  private void updateRouter() throws IOException {
    Peer peer = null;
    if (null == abortedSegment) {
      router.update(null);
    } else {
      peer = router.locateSegment(abortedSegment);
      router.update(peer);
      abortedSegment = null;
    }
    while (!router.checkComplete()) {
      LOG.info("Routing information is incomplete.  Waiting...");
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        // ignore
      }
      router.update(peer);
    }
  }

  
  private boolean tryFlush() throws IOException {
    
    //debug();
    
    InetSocketAddress[] addrs = getAddresses(peers.keySet());
    
    try {
    
      Object[] replies;
    
      while (true) {
        long time = System.currentTimeMillis();
        // if this returns FALSE for any peer then there wasn't
        // enough transaction buffer space.  simply retry.
        replies = startTransactions(addrs);
        if (checkReplies(replies)) {
          break; // all good.
        }
        timeStalled += System.currentTimeMillis() - time;
        LOG.info("Couldn't start transactions.  Retrying...");
        abort(addrs);
      }
      
      replies = writeAndPrepare(addrs);
      if (!checWritekReplies(replies)) {
        abort(addrs);
        return false;
      }
      
      commit(addrs);
    
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    
    clear();
    
    return true;
  }
  
  
  
  
    
  
  private Object[] startTransactions(InetSocketAddress[] addrs) 
  throws  IOException, InterruptedException {
   
    Object[][] params = new Object[addrs.length][];
    int i = 0;
    for (MultiSegmentWrite write : peers.values()) {
      params[i++] = new Object[]{transactionId, write.getSizeBytes(), 10 * 1000L};
    }
    
    // do parallel calls
    return RPC.call(startTransactions, params, addrs, null, router.getConf());
  }
  
  
  private Object[] writeAndPrepare(InetSocketAddress[] addrs) 
  throws IOException, InterruptedException {
    
    Object[][] params = new Object[addrs.length][]; 
    int i = 0;
    for (MultiSegmentWrite write : peers.values()) {
      params[i++] = new Object[]{transactionId, write};
    }
    
    // do parallel call
    return RPC.call(writeAndPrepare, params, addrs, null, router.getConf());
  }
  
  
  private void commit(InetSocketAddress[] addrs) 
  throws IOException, InterruptedException {
    
    Object[][] params = new Object[addrs.length][];
    Arrays.fill(params, new Object[]{transactionId});
    
    // do parallel call
    RPC.call(commit, params, addrs, null, router.getConf());
  }
  
  
  private boolean checkReplies(Object[] replies) {
    for (int i=0; i<replies.length; ++i) {
      if (NodeProtocol.FALSE.equals(replies[i])) {
        return false;
      }
    }
    return true;
  }
  
  private boolean checWritekReplies(Object[] replies) {
    for (int i=0; i<replies.length; ++i) {
      TransactionState reply = (TransactionState) replies[i];
      // TODO in case of a network error, reply can be null... need to cover this.
      if (!reply.isOk()) {
        if (0 != reply.getSegmentId()) {
          for (SegmentInfo segment : segments.keySet()) {
            if (segment.getSegmentId() == reply.getSegmentId()) {
              abortedSegment = segment;
              break;
            }
          }
        }
        return false;
      }
    }
    return true;
  }
  
  
  private void abort(InetSocketAddress[] addrs) 
  throws IOException, InterruptedException {
    
    Object[][] params = new Object[addrs.length][];
    Arrays.fill(params, new Object[]{transactionId});
    
    // do parallel call
    RPC.call(abortTransaction, params, addrs, null, router.getConf());
  }
  
  
  static InetSocketAddress[] getAddresses(Set<Peer> peers) {
    InetSocketAddress[] addr = new InetSocketAddress[peers.size()];
    int i=0;
    for (Peer peer : peers) {
      addr[i++] = peer.getConnectAddress();
    }
    return addr;
  }
  
  
  void debug() {
    
    for (Map.Entry<Peer, MultiSegmentWrite> entry : peers.entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
    
  }
  
  
  public long getTimeStalled() {
    return timeStalled;
  }
  
  public long getTimeRouterUpdate() {
    return timeRouterUpdate;
  }
  
  public long getAbortedTransactions() {
    return abortedTransactions;
  }
  
}
