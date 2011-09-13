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

import java.io.IOException;

import org.apache.hadoop.ipc.RPC;

import de.hpi.fgis.hdrs.Configuration;
import de.hpi.fgis.hdrs.Peer;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.ipc.NodeProtocol;
import de.hpi.fgis.hdrs.ipc.ScanResult;
import de.hpi.fgis.hdrs.tio.TripleScanner;

/**
 * Scanner for scanning a segment.  This type of scanner is used inside the 
 * ClientIndexScanner, which is the scanner that client applications should
 * use.
 * 
 * @author daniel.hefenbrock
 *
 */
public class ClientSegmentScanner extends TripleScanner {

  private NodeProtocol proxy;
  private final long scannerId;
  private final Triple.COLLATION order;
  
  private TripleScanner scanner;
  private boolean aborted = false;
  private boolean done = false;
  
  
  private ClientSegmentScanner(NodeProtocol proxy, ScanResult scan) 
  throws IOException {
    this.proxy = proxy;
    this.scannerId = scan.getScannerId();
    this.order = scan.getOrder();
    this.scanner = null==scan.getTriples() ? null : scan.getTriples().getScanner();
    this.done = scan.isDone();
  }
  
  static ClientSegmentScanner open(Configuration conf, long segmentId, Peer peer,
      Triple pattern, boolean filterDeletes, Triple seek) 
  throws IOException {
    NodeProtocol proxy = peer.getProxy(conf);
    return open(conf, segmentId, proxy, pattern, filterDeletes, seek);
  }
  
  /**
   * <p>Constructs a scanner for a single segment.  The scanner stops at the end of
   * the segment or possibly earlier if a pattern is supplied.
   * 
   * <p>This method is for EXPERTS, normal users should use the ClientIndexScanner
   * that abstracts from segments and instead scans an index.
   * 
   * @param conf  Configuration to use.
   * @param segmentId The segment ID of the segment to be scanned.
   * @param proxy A proxy object for the HDRS node where the segment is stored.
   * @param pattern A pattern to be matched by the scanner.
   * @param filterDeletes If true, triples with multiplicity < 0 are skipped.
   * @param seek  A triple where to start scanning.
   * @return  A scanner, or null if the segment ID is invalid.
   * @throws IOException
   */
  public static ClientSegmentScanner open(Configuration conf, long segmentId, NodeProtocol proxy,
      Triple pattern, boolean filterDeletes, Triple seek) 
  throws IOException {
    ScanResult scan = proxy.openScanner(segmentId, filterDeletes, 0, pattern, seek);
    if (scan.isClosed()) {
      RPC.stopProxy(proxy);
      proxy = null;
    }
    if (scan.isInvalid()) {
      return null;
    }
    return new ClientSegmentScanner(proxy, scan);
  }
  
  @Override
  public COLLATION getOrder() {
    return order;
  }

  @Override
  protected Triple nextInternal() throws IOException {
    while (null == scanner || !scanner.next()) {
      if (null == proxy) {
        return null;
      }
      ScanResult scan = proxy.next(scannerId, 0);
      if (scan.isClosed()) {
        if (scan.isExpired()) {
          // scanner timed out... 
          throw new ScannerTimeoutException();
        } else {
          RPC.stopProxy(proxy);
          proxy = null;
          aborted = scan.isAborted();
          done = scan.isDone();
        }
      }
      scanner = scan.getTriples().getScanner();
    }
    return scanner.pop();
  }

  @Override
  public void close() throws IOException {
    if (null != proxy) {
      proxy.close(scannerId);
      RPC.stopProxy(proxy);
      proxy = null;
    }
    scanner = null;
  }
  
  public boolean isAborted() {
    return aborted;
  }
  
  public boolean isDone() {
    return done;
  }
  
  static class ScannerTimeoutException extends IOException {

    /**
     * 
     */
    private static final long serialVersionUID = 3938901464327862954L;

  }

}
