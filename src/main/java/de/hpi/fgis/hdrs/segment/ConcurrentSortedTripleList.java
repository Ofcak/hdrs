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

package de.hpi.fgis.hdrs.segment;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.tio.IteratorScanner;
import de.hpi.fgis.hdrs.tio.SeekableTripleSource;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.tio.TripleSink;

/**
 * <p> This class implements an in-memory list of triples with the following 
 * properties:
 * 
 * <ul><li> Triples, when read using the scanner provided by getScanner(),
 * are ordered according to a at creation time specified collation order.
 * <li> Triples can be added using add() in any order.
 * <li> Adding using add() triples is thread-safe.
 * <li> The list can be scanned while concurrently triples are added,
 * e.g. by other threads, without producing ConcurrentModificationExceptions
 * and likes.
 * </ul>
 * 
 * <p> The list is internally backed by a ConcurrentSkipListMap, which
 * dictates the consistency model: "[Scanners] are weakly consistent, 
 * returning elements reflecting the state of the [list] at some point at or 
 * since the creation of the [Scanner]."
 * 
 * <p> This list provides a special "search scanner" that is designed to
 * work well while the list is updated by other threads.
 * 
 * @author daniel.hefenbrock 
 *
 */
class ConcurrentSortedTripleList implements TripleSink, SeekableTripleSource {
  
  private final ConcurrentSkipListMap<Triple, Triple> index;
  
  
  public ConcurrentSortedTripleList(Triple.COLLATION order) {
    index = new ConcurrentSkipListMap<Triple, Triple>(order.comparator());
  }
  
  
  /**
   * Add a triple to this list.  If the an equal triple is already present,
   * the multiplicity will be updated accordingly.
   * @param triple  The triple to add.
   * @return  True, if the triple was not already present, false if only
   * the multiplicity was updated.
   */
  @Override
  public boolean add(Triple triple) {
    Triple old = index.putIfAbsent(triple, triple);
    if (null != old) {
      synchronized (old) {
        old.merge(triple);
      }
      return false;
    }
    return true;
  }
  
  
  @Override
  public TripleScanner getScanner() {
    return new CSTLIteratorScanner(index.keySet().iterator());
  }
  
  
  /**
   * This scanner does not filter 0-multiplicity triples.
   * (unlike the getScanner() scanner)
   */
  public TripleScanner getRawScanner() {
    return new IteratorScanner(index.keySet().iterator());
  }
  
  
  @Override
  public TripleScanner getScanner(Triple pattern) throws IOException {
    TripleScanner scanner = new CSTLIteratorScanner(index.tailMap(pattern).keySet().iterator());
    if (scanner.next()) {
      return scanner;
    }
    return null;
  }
  
  
  @Override
  public TripleScanner getScannerAt(Triple pattern) throws IOException {
    TripleScanner scanner = new CSTLIteratorScanner(index.tailMap(pattern).keySet().iterator());
    if (scanner.next() && 
        getOrder().comparator().match(scanner.peek(), pattern)) {
      return scanner;
    }
    return null;
  }
  
  
  @Override
  public TripleScanner getScannerAfter(Triple pattern) throws IOException {
    TripleScanner scanner = new CSTLIteratorScanner(index.tailMap(pattern, 
        false).keySet().iterator());
    if (scanner.next()) {
      return scanner;
    }
    return null;
  }
  
  
  public boolean contains(Triple pattern) {
    Triple triple = index.ceilingKey(pattern);
    if (null == triple){
      return false;
    }
    return ((Triple.Comparator) index.comparator()).match(
        pattern, triple);
  }
  
  
  public boolean containsAfter(Triple pattern) {
    return null != index.higherKey(pattern);
  }
  
  
  public boolean isEmpty() {
    return index.isEmpty();
  }
  
  
  private class CSTLIteratorScanner extends TripleScanner {

    private Iterator<Triple> iterator;
    
    public CSTLIteratorScanner(Iterator<Triple> it) {
      iterator = it;
    }
    
    @Override
    public COLLATION getOrder() {
      return ((Triple.Comparator) index.comparator()).getCollation();
    }

    @Override
    protected Triple nextInternal() throws IOException {
      while (iterator.hasNext()) {
        Triple next = iterator.next();
        if (0 != next.getMultiplicity()) {
          return next;
        }
      }
      return null;
    }

    @Override
    public void close() throws IOException {
    }
    
    @Override
    public String toString() {
      return "CSTLIteratorScanner(" + ConcurrentSortedTripleList.this + ")";
    }
    
  }
  

  @Override
  public COLLATION getOrder() {
    return ((Triple.Comparator) index.comparator()).getCollation();
  }

}
