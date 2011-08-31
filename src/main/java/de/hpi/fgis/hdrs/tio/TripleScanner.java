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

import java.io.Closeable;
import java.io.IOException;

import de.hpi.fgis.hdrs.Triple;

/**
 * This base class defines the general contract for triple scanners used
 * inside segments to iterate through a sorted collection of triples.
 * 
 * @author hefenbrock
 *
 */
public abstract class TripleScanner implements Closeable {
  
  private Triple next;
  
  /**
   * <p> Retrieve the next triple and pop it off the iterator.
   * A subsequent call to next() will fetch the next triple, if any,
   * of the underlying ordered collection.
   * 
   * <p> Calling pop() without calling next() in before, or if
   * next() returned false, causes an exception to be thrown.
   * 
   * @return  The triple previously fetched by next();
   */
  public final Triple pop() {
    if (null == next) {
      throw new IllegalStateException("pop() called with no triple present");
    }
    Triple t = next;
    next = null;
    return t;
  }
  
  /**
   * <p> Retrieve the next triple without removing it from the scanner.
   * A subsequent call to next() will NOT fetch the next triple.
   * 
   * @return  The triple previously fetched by next();
   */
  public final Triple peek() {
    return next;
  }

  /**
   * <p> Try to fetch the next triple from the underlying ordered
   * collection.  Note that, unlike pop() and peek(), this method
   * can ALWAYS be called without raising an exception, except for
   * I/O related exceptions from lower layers.
   * 
   * @return True, if a triple was fetched successfully, or if
   * a previously fetched triple wasn't removed by calling pop()
   * afterwards, thus, is still present.
   * @throws IOException
   */
  public final boolean next() throws IOException {
    if (null != next) {
      return true;
    }
    next = nextInternal();
    return null != next;
  }
  
  /**
   * This is where the magic happens, to be implemented by subclasses.
   * @return The next triple, or null, when no more triples available.
   * @throws IOException
   */
  protected abstract Triple nextInternal() throws IOException;
  
  /**
   * @return The collation order of the underlying collection of triples.
   */
  public abstract Triple.COLLATION getOrder();
  
  
  /**
   * <p> Seek this scanner to the first triple matching pattern.  If there 
   * is no such triple, the scanner will be positioned at the first
   * triple greater than pattern.
   * 
   * @param pattern  Pattern to match.
   * @return  True if a triple matching pattern was found.
   * @throws IOException
   */
  public boolean seek(Triple pattern) throws IOException {
    Triple.Comparator comparator = getOrder().comparator();
    while (next()) {
      int cmp = comparator.compare(peek(), pattern);
      if (0 < cmp) {
        // current triple is greater than pattern
        return comparator.match(peek(), pattern);
      } else if (0 == cmp) {
        // exact match
        return true;
      }
      pop();
    }
    return false;
  }
  
  
  /**
   * <p> Seek this scanner to the first triple greater than the given
   * triple.  
   * 
   * @param triple
   * @return  True, if there is a triple greater than the given triple.
   * @throws IOException
   */
  public boolean seekAfter(Triple triple) throws IOException {
    assert(!triple.isPattern()); // for now
    if (seek(triple)) {
      pop();
    }
    return next();
  }
  
  
  @Override
  public String toString() {
    return "TripleScanner(order = " + getOrder()
        + ", next = " + (null==next?"?":next) + ")";
  }
  
}
