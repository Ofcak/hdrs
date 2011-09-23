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

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.client.ClientSegmentScanner.ScannerTimeoutException;
import de.hpi.fgis.hdrs.routing.Router;
import de.hpi.fgis.hdrs.routing.SegmentInfo;
import de.hpi.fgis.hdrs.tio.TripleScanner;

public class ClientIndexScanner extends TripleScanner {

  private final Router router;
  private final Triple.COLLATION index;
  private final Triple pattern;
  private final Triple rangeStart;
  private final Triple rangeEnd;
  private final boolean filterDeletes;
  
  private SegmentInfo segment = null;
  private ClientSegmentScanner scanner = null;
  
  private Triple prev = null;
  
  
  ClientIndexScanner(Router router, Triple.COLLATION index, Triple pattern, 
      boolean filterDeletes) 
  throws IOException {
    this(router, index, pattern, filterDeletes, 
        Triple.MAGIC_TRIPLE, Triple.MAGIC_TRIPLE);
  }
  
  
  ClientIndexScanner(Router router, Triple.COLLATION index, Triple pattern,
      boolean filterDeletes, Triple rangeStart, Triple rangeEnd) 
  throws IOException {
    if (null == rangeStart || null == rangeEnd) {
      throw new IllegalArgumentException("range start and end cannot be null");
    }
    this.router = router;
    this.index = index;
    this.pattern = pattern;
    this.filterDeletes = filterDeletes;
    this.rangeStart = rangeStart;
    this.rangeEnd = rangeEnd;
    
    openScanner(getStartTriple());
  }
  
  
  private Triple getStartTriple() {
    // if rangeStart is after pattern, we seek there.
    // otherwise, we seek to pattern. (unless pattern is null)
    return null == pattern || 0 < index.magicComparator().compare(rangeStart, pattern) 
        ? rangeStart : pattern;
  }
  
  
  @Override
  public COLLATION getOrder() {
    return index;
  }

  
  private boolean tryNext() throws IOException {
    try {
      return scanner.next();
    } catch (ScannerTimeoutException ex) {
      Client.LOG.info("Scanner timeout. Reopening scanner...");
      openScanner(prev == null ? getStartTriple() : prev);
      return scanner.next();
    }
  }
  
  
  @Override
  protected Triple nextInternal() throws IOException {
    if (scanner == null) {
      return null;
    }
    while (!tryNext()) {
      Triple seek = null;
      
      if (scanner.isDone()) {
        scanner = null;
        close();
        return null;
      } else if (scanner.isAborted()) {
        seek = prev == null ? getStartTriple() : prev;
        // the current will not be the previous segment of the segment
        // we are going to open.  null it, otherwise we end up in a loop.
        segment = null;
      } else {
        // segment is over
        seek = segment.getHighTriple();
        if (seek.equals(rangeEnd)) {
          // this was the last segment
          scanner = null;
          close();
          return null;
        }
      }
      
      openScanner(seek);
    }
    
    prev = scanner.pop();
    if (!rangeEnd.isMagic()) {
      if (0 < index.comparator().compare(prev, rangeEnd)) {
        // done with range
        scanner = null;
        close();
        return null;
      }
    }
    return prev;
  }
  
  
  private void openScanner(Triple seek) throws IOException {
    do {
      if (null == scanner && null != segment) {
        // previous try was aborted... segment is probably split
        Client.LOG.info("Invalid segment.  Updating routing information.");
        try {
          Thread.sleep(250);
        } catch (InterruptedException e) {
          // ignore
        }
        router.update(router.locateSegment(index, segment));
        // start over (segment is NOT the previous segment)
        segment = null;
      }
      
      // figure out which segments to scan      
      SegmentInfo[] segments = router.getIndex(index, 
          null == seek ? Triple.MAGIC_TRIPLE : seek);
            
      for (SegmentInfo s : segments) {
        // take the first segment that we haven't read already.
        if (null != segment && !segment.getHighTriple().equals(s.getLowTriple())) {
          continue;
        }
        segment = s;
        scanner = ClientSegmentScanner.open(router.getConf(), 
            segment.getSegmentId(), router.locateSegment(index, segment), 
            pattern, filterDeletes, seek);
        
        if (null == scanner) {
          // scanner can only be null if the segment doesn't exist.
          // need to update the router
          break;
        }
        if (!scanner.isDone()) {
          // if this scanner is done we need to look at the subsequent
          // segment
          break;
        }
      }
      
    } while (null == scanner);
  }
  
  
  @Override
  public void close() throws IOException {
    if (scanner != null) {
      scanner.close();
    }
    segment = null;
    prev = null;
  }

}
