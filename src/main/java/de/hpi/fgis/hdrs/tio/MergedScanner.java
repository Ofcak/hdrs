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

package de.hpi.fgis.hdrs.tio;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;

/**
 * <p> A scanner that merges a number of equally ordered triple scanners 
 * into one triple scanner while maintaining the order.
 * 
 * <p> This scanner essentially performs a merge-join of the underlying
 * scanners.  This is fast because all underlying scanners are ordered
 * in the same fashion.
 * 
 * <p> Moreover, this scanner merges identical triples contained in 
 * different input scanners by adding up their multiplicities. 
 * Triples are skipped by this scanner if the resulting multiplicity
 * is <= 0.
 * 
 * @author hefenbrock
 *
 */
public class MergedScanner extends TripleScanner {

  private final TripleScanner[] scanners;
  
  private PriorityQueue<TripleScanner> scannerHeap = null;
  
  private final Triple.COLLATION order;
  
  
  private final Comparator<TripleScanner> COMPARATOR = 
    new Comparator<TripleScanner>() {
    
        @Override
        public int compare(TripleScanner s1, TripleScanner s2) {
          return order.comparator().compare(s1.peek(), s2.peek());
        }
        
    };
    
  
  public MergedScanner(TripleScanner...scanners) {
    this.scanners = scanners;
    // check: at least 2 scanners?
    if (2 > scanners.length) {
      throw new IllegalArgumentException("need at least two scanners!");
    }
    // check: do all underlying files have the same order?
    for (int i=1; i<scanners.length; ++i) {
      if (scanners[0].getOrder() != scanners[i].getOrder()) {
        throw new IllegalArgumentException("scanners have different orderings!");
      }
    }
    order = scanners[0].getOrder();
  }

  @Override
  public COLLATION getOrder() {
    return order;
  }

  
  private void initialize() throws IOException {
    scannerHeap = new PriorityQueue<TripleScanner>(scanners.length, COMPARATOR);
    for (int i=0; i<scanners.length; ++i) {
      if (!scanners[i].next()) {
        // skip empty scanners
        continue;
      }
      scannerHeap.add(scanners[i]);
    }
  }
  
  
  private Triple popTriple() throws IOException {
    // get top scanner
    TripleScanner scanner = scannerHeap.poll();
    // remove next triple
    Triple next = scanner.pop();
    // more triples in this scanner?
    if (scanner.next()) {
      // yep, put it back
      scannerHeap.add(scanner);
    } else {
      // nope, we're done with it
      scanner.close();
    }
    return next;
  }
  
  
  @Override
  protected Triple nextInternal() throws IOException {
    
    // next is null
    
    if (null == scannerHeap) {
      initialize();
    }
        
    while (!scannerHeap.isEmpty()) {
      
      // get next triple
      Triple next = popTriple();
      
      while (!scannerHeap.isEmpty()) {
        Triple peek = scannerHeap.peek().peek();
        
        if (!order.comparator().match(next, peek)) {
          break;
        }
        // remove top scanner
        popTriple();
        // next and peek match... merge them
        next = Triple.merge(next, peek);
        // keep on merging until all matches are done.
        if (null == next) {
          // triples and deletes canceled each other out
          break;
        }
      }
      
      // are we done? ignore left over deletes
      // EDIT: deletes are filtered in another layer.
      if (null != next /*&& !next.isDelete()*/) {
        return next;
      }
    }
    
    // all scanners done
    return null;
  }
  

  @Override
  public void close() throws IOException {
    // all other scanners are closed already
    for (TripleScanner scanner : scannerHeap) {
      scanner.close();
    }
  }
  
  
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder("MergedScanner(");
    for (int i=0; i<scanners.length; ++i) {
      str.append(scanners[i]);
      if (i<scanners.length-1) {
        str.append(", ");
      }
    }
    str.append(")");
    return str.toString();
  }
  
}
