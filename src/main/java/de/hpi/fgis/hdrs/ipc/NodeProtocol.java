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

package de.hpi.fgis.hdrs.ipc;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;

import de.hpi.fgis.hdrs.StoreInfo;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.routing.SegmentInfo;
import de.hpi.fgis.hdrs.triplefile.FileBlock;

public interface NodeProtocol extends NodeManagementProtocol {

  
  public static final BooleanWritable TRUE = new BooleanWritable(true);
  
  public static final BooleanWritable FALSE = new BooleanWritable(false);
  
  
  public StoreInfo getStoreInfo();
  
  
  //
  //  Reading / Scanning
  //
  
  public ScanResult openScanner(long segmentId, int limit) 
  throws IOException;
  
  public ScanResult openScanner(long segmentId, boolean filterDeletes, int limit) 
  throws IOException;
  
  public ScanResult openScanner(long segmentId, boolean filterDeletes, int limit, 
      Triple pattern) 
  throws IOException;
  
  public ScanResult openScanner(long segmentId, boolean filterDeletes, int limit, 
      Triple pattern, Triple seek) 
  throws IOException;
  
  public ScanResult next(long scannerId, int limit) 
  throws IOException;
  
  public void close(long scannerId) throws IOException;
  
  
  //
  //  Transactional Writing and Flow Control 
  //
  
  /**
   * <p> Initiate a new transaction identified by transactionId.  There must not
   * be another transaction with the same id on this node.
   * 
   * <p> bufferSize determines how much memory the node should reserve for this
   * transaction.  
   * 
   * @return A byte indicating the transaction state: OPEN, if transaction was
   * successfully started, ABORTED if not.  The reason for ABORTED is that the
   * node cannot handle a transaction with requested buffer size at this point.
   * Retrying with a smaller buffer size is recommended.
   * @throws IOException
   */
  public BooleanWritable startTransaction(
      long transactionId,
      int bufferSize,
      long timeout) throws IOException;
  
  /**
   * <p> Write triples to the segment identified by segmentId, as part of the
   * transaction identified by transactionId.  The transaction MUST be in state
   * OPEN.
   * 
   * <p> If the size of the triples exceeds the previously requested buffer 
   * size then this transaction is automatically aborted (returns ABORTED).
   * 
   * <p> ABORTED is also returned when one of the segments of this transaction
   * was split since the transaction was started.
   * 
   * <p> If PREPARED is returned this means that this transaction cannot take
   * additional writes but must be committed or aborted.  The reason for this
   * is either memory pressure on the node or node policy to keep transactions
   * at a reasonable size.
   * 
   * @return The transaction state.
   * @throws IOException
   */
  public TransactionState writeTriples(
      long transactionId, 
      MultiSegmentWrite writes) throws IOException;
  
  /**
   * <p> Prepare this transaction for being committed.  Must be in OPEN state.
   * 
   * <p> PREPARED is returned in case of success.  A transaction in PREPARED
   * state will never be aborted by the node but can still be aborted by the
   * caller.  This means that segments splits have to wait for PREPARED trans-
   * actions to be committed first.  It is, therefore, imperative that commit
   * (or abort) be called soon after prepare in order for the node to function 
   * properly.
   * 
   * <p> ABORTED is returned if the transaction was aborted by the node earlier,
   * e.b. because of a segment split or transaction timeout.
   * 
   * @return
   * @throws IOException
   */
  public TransactionState prepareTransaction(
      long transactionId) throws IOException;
  
  // save one rountrip for short transactions
  public TransactionState writeAndPrepare(
      long transactionId, 
      MultiSegmentWrite writes) throws IOException;
  
  
  public void commitTransaction(
      long transactionId) throws IOException;
  
  
  public void abortTransaction(
      long transactionId) throws IOException;
  
  
  //
  //  Segment catalog / Anti-entropy
  //
  
  public SegmentCatalogUpdates antiEntropy(long[] versionVector) throws IOException;
  
  //
  //  Segment transfer
  //
  
  public void startSegmentTransfer(SegmentInfo segment, byte collation) throws IOException;
  
  public void startFileTransfer(long segmentId, byte compressionAlgorithm,
      long dataSize, long nTriples) throws IOException;
  
  public void transferFileBlocks(long segmentId, FileBlock block[]) throws IOException;
  
  public void finishFileTransfer(long segmentId, Triple lastTriple) throws IOException;
  
  public boolean finishSegmentTransfer(long segmentId) throws IOException;
  
}
