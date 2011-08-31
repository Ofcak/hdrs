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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;

import de.hpi.fgis.hdrs.Configuration;
import de.hpi.fgis.hdrs.Peer;
import de.hpi.fgis.hdrs.StoreInfo;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.client.TripleOutputStream.TransactionIdGenerator;
import de.hpi.fgis.hdrs.ipc.NodeProtocol;
import de.hpi.fgis.hdrs.routing.Router;
import de.hpi.fgis.hdrs.routing.SegmentInfo;
import de.hpi.fgis.hdrs.tio.TripleScanner;

/**
 * <p>The HDRS client application interface.  Applications must use this class
 * to read/write triples to/from a HDRS store.
 * 
 * <p>The client class also provides some advanced features to provide client
 * applications with the index segmentation or location of segments.
 * 
 * <p>Note the client interface is for single-threaded use.  If client 
 * application wishes to use several threads, each thread MUST be use its
 * own client object.
 * 
 * @author daniel.hefenbrock
 *
 */
public class Client implements Closeable {

  static final Log LOG = LogFactory.getLog(Client.class);
  
  private final Router router;
  private final int id;
  private final TransactionIdGenerator txIdGen = new IdGenerator();
  private NodeProtocol rpcDummy;
  
  /**
   * Create a client interface object.
   * @param conf  A configuration to be used
   * @param store  A node address in the form of "hostname[:port]"
   * @throws IOException
   */
  public Client(Configuration conf, String store) throws IOException {
    this(conf, Configuration.loadStoreInfo(conf, store));
  }
  
  private Client(Configuration conf, StoreInfo info) 
  throws IOException {
    this(info.getClientId(), conf, info.getIndexes(), info.getPeers());
  }
  
  /**
   * <p>Create a client object from a local configuration.
   * 
   * <p>EXPERTS only.
   * 
   * @param id  Client id be used.
   * @param indexFilePath Path to index configuration file
   * @param peerFilePath  Path to peers configuration file
   * @throws IOException
   */
  public Client(int id, String indexFilePath, String peerFilePath) throws IOException {
    this(id, Configuration.create(), 
        Configuration.readIndexes(indexFilePath), 
        Configuration.readPeers(peerFilePath));
  }
  
  public Client(int id, Configuration conf,
      Set<Triple.COLLATION> indexes, ArrayList<Peer> peers) throws IOException {
    this.id = id;
    router = new Router(conf, peers);
    for (Triple.COLLATION index : indexes) {
      router.initialize(index);
    }
    // nasty workaround for hadoop rpc
//    rpcDummy = RPC.getProxy(RpcDummy.class, 0, null, conf);
    rpcDummy = router.getPeers().get(0).getProxy(conf);
    LOG.info("Updating routing information");
    router.update(null);
  }
  
  /**
   * Query which indexes are present.
   * @return  A set of indexes that are present at the HDRS store.
   */
  public Set<Triple.COLLATION> getIndexes() {
    return router.getCollations();
  }
  
  /**
   * Create an output stream where triples can be added to the store.
   */
  public TripleOutputStream getOutputStream() {
    return new TripleOutputStream(router, txIdGen, router.getCollations());
  }
  
  /**
   * Create an output stream that writes triples only to a specified 
   * subset of indexes.
   * @param indexes  A set of indexes to be written.
   * @throws IllegalArgumentException If any of the indexes is not present in the store.
   */
  public TripleOutputStream getOutputStream(Set<Triple.COLLATION> indexes) {
    for (Triple.COLLATION index : indexes) {
      if (!getIndexes().contains(index)) {
        throw new IllegalArgumentException("Index " + index + " is not present");
      }
    }
    return new TripleOutputStream(router, txIdGen, indexes);
  }
  
  
  /**
   * Get a scanner for an index.  The scanner starts at the first triple
   * contained in the index.
   * @return A scanner.
   * @throws IOException
   */
  public TripleScanner getScanner(Triple.COLLATION index) throws IOException {
    return new ClientIndexScanner(router, index, null, true);
  }
  
  /**
   * Get a scanner for an index that scans triples matching a 
   * particular pattern.
   * @param index  The index to be scanned
   * @param pattern  The pattern to be matched
   * @return The scanner
   * @throws IOException
   */
  public TripleScanner getScanner(Triple.COLLATION index, Triple pattern) throws IOException {
    return new ClientIndexScanner(router, index, pattern, true);
  }
  
  /**
   * <p>Get a scanner for an index that scans triples matching a 
   * particular pattern.
   * <p>The scanner can be configured as to whether it skips triples
   * having a negative multiplicity, or not.
   * @param index The index to be scanned
   * @param pattern The pattern to be matched
   * @param filterDeletes This flag specifies whether to filter out
   * triples, or not.
   * @return The scanner
   * @throws IOException
   */
  public TripleScanner getScanner(Triple.COLLATION index, Triple pattern,
      boolean filterDeletes) throws IOException {
    return new ClientIndexScanner(router, index, pattern, filterDeletes);
  }
  
  /**
   * <p>Get a scanner for an index that scans triples matching a 
   * particular pattern.
   * <p>Further, a range can be specified.  The range defines
   * where the scanner starts and stops.
   * @param index  The index to be scanned
   * @param pattern  The pattern to be matched
   * @param rangeStart  The start of the triple range (inclusive)
   * @param rangeEnd  The end of the triple range (exclusive)
   * @return The scanner
   * @throws IOException
   */
  public TripleScanner getScanner(Triple.COLLATION index, Triple pattern,
      Triple rangeStart, Triple rangeEnd) 
  throws IOException {
    return new ClientIndexScanner(router, index, pattern, true, rangeStart, rangeEnd);
  }
  
  /**
   * <p>Get a scanner for an index that scans triples matching a 
   * particular pattern.
   * <p>Further, a range can be specified.  The range defines
   * where the scanner starts and stops.
   * <p>The scanner can be configured as to whether it skips triples
   * having a negative multiplicity, or not.
   * @param index  The index to be scanned
   * @param pattern  The pattern to be matched
   * @param filterDeletes This flag specifies whether to filter out
   * triples, or not.
   * @param rangeStart  The start of the triple range (inclusive)
   * @param rangeEnd  The end of the triple range (exclusive)
   * @return The scanner
   * @throws IOException
   */
  public TripleScanner getScanner(Triple.COLLATION index, Triple pattern,
      boolean filterDeletes, Triple rangeStart, Triple rangeEnd) 
  throws IOException {
    return new ClientIndexScanner(router, index, pattern, filterDeletes, rangeStart, rangeEnd);
  }

  /**
   * Closes the connection to the HDRS store.
   */
  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcDummy);
  }
  
  /**
   * Query peers in the HDRS store.
   * @return  A list of peers.
   */
  public ArrayList<Peer> getPeers() {
    return router.getPeers();
  }
  
  /**
   * Query the index segmentation of an index.
   * @param index  The index to query.
   * @return  A sorted array of SegmentInfos for segments that 
   * are present in the index, in the order defined by the index.
   */
  public SegmentInfo[] getIndex(Triple.COLLATION index) {
    return router.getIndex(index);
  }
  
  /**
   * Query the index segmentation of an index, restricted
   * to a particular pattern to be matched.
   * @param index  The index to query.
   * @param pattern  The pattern to be matched.
   * @return  A sorted array of SegmentInfos for segments that 
   * are present in the index, in the order defined by the index.
   */
  public SegmentInfo[] getIndex(Triple.COLLATION index, Triple pattern) {
    return router.getIndex(index, pattern);
  }
  
  /**
   * Query the index segmentation of an index, restricted
   * to a particular range of triples.
   * @param index  The index to query.
   * @param start  The start triple of the range (inclusive).
   * @param stop  The end triple of the range (exclusive).
   * @return  A sorted array of SegmentInfos for segments that 
   * are present in the index, in the order defined by the index.
   */
  public SegmentInfo[] getIndexRange(Triple.COLLATION index, Triple start, Triple stop) {
    return router.getIndexRange(index, start, stop);
  }
  
  /**
   * Locate a segment.
   * @param segment  The segment to be located.
   * @return  The peer where the segment is located.
   */
  public Peer locateSegment(SegmentInfo segment) {
    return router.locateSegment(segment);
  }
  
//  private static interface RpcDummy {
//    
//  }
  
  private class IdGenerator implements TransactionIdGenerator {

    private int txId = 0;
    
    @Override
    public synchronized long nextTransactionId() {
      return (((long) Client.this.id) << 31) + txId++;
    }
    
  }
  
}
