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

package de.hpi.fgis.hdrs.routing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.Comparator;
import de.hpi.fgis.hdrs.Triple.MagicComparator;

/**
 * In-memory structure that holds all segments contained in one index
 * in an ordered list for fast lookups.
 * 
 * @author daniel.hefenbrock
 *
 */
class SegmentIndex {

  final NavigableMap<Triple, SegmentInfo> index;
  
  
  SegmentIndex(Triple.COLLATION order) {
    index = new TreeMap<Triple, SegmentInfo>(order.magicComparator());
  }
  
  
  private void addAll(List<SegmentInfo> segments) {
    for (SegmentInfo s : segments) {
      index.put(s.getLowTriple(), s);
    }
  }
  
  private void removeAll(List<SegmentInfo> segments) {
    for (SegmentInfo s : segments) {
      SegmentInfo removed = index.remove(s.getLowTriple());
      if (null != removed && removed.getSegmentId() != s.getSegmentId()) {
        // after a segment transfer-in, we add the segment to the index
        // in updateLocalCatalog().  later, once we receive the update from
        // the node where the segment was transferred from, we cannot 
        // remove this segment since it was REPLACED earlier.
        index.put(s.getLowTriple(), removed);
        // simply puttin it back is not a nice solution, but it works.
        // TODO implement more elegantly.
      }
    }
  }
  
  
  /**
   * @return  The segment id for triple t.
   */
  public synchronized SegmentInfo locate(Triple t) {
    Map.Entry<Triple, SegmentInfo> entry = index.floorEntry(t);
    if (null == entry) {
      entry = index.firstEntry();
    }
    return entry.getValue();
  }
  
  
  public synchronized boolean contains(SegmentInfo info) {
    SegmentInfo i = index.get(info.getLowTriple());
    if (null == i) {
      return false;
    }
    return info.equals(i);
  }
  
  
  /**
   * Add segments in the catalog to the index.  Before that, remove old ones.
   * 
   * @param catalog
   */
  public void update(List<SegmentInfo> oldList, List<SegmentInfo> newList) {
    
    if (null == oldList) {
      synchronized (index) {
        addAll(newList);
      }
    } else {
      // compute "diff"
      Iterator<SegmentInfo> oldIt = oldList.iterator();
      Iterator<SegmentInfo> newIt = newList.iterator();
      
      List<SegmentInfo> add = new ArrayList<SegmentInfo>();
      List<SegmentInfo> rem = new ArrayList<SegmentInfo>();
      SegmentInfo oldSeg = null;
      SegmentInfo newSeg = null;
      
      while ((oldIt.hasNext() || null != oldSeg) 
          && (newIt.hasNext() || null != newSeg)) {
        if (null == oldSeg) {
          oldSeg = oldIt.next();
        }
        if (null == newSeg) {
          newSeg = newIt.next();
        }
        int cmp = index.comparator().compare(
            oldSeg.getLowTriple(), newSeg.getLowTriple());
        if (0 == cmp) {
          if (oldSeg.getSegmentId() != newSeg.getSegmentId()) {
            // same high triples, but different segment ids... replace
            add.add(newSeg);
            rem.add(oldSeg);
          }
          newSeg = null;
          oldSeg = null;
        } else if (0 < cmp) {
          // old segment triple is larger, add new one
          add.add(newSeg);
          newSeg = null;
        } else {
          // new segment triple is larger, remove old one
          rem.add(oldSeg);
          oldSeg = null;
        }
      }
      
      while (oldIt.hasNext() || null != oldSeg) {
        if (null == oldSeg) {
          oldSeg = oldIt.next();
        }
        rem.add(oldSeg);
        oldSeg = null;
      }
      
      while (newIt.hasNext() || null != newSeg) {
        if (null == newSeg) {
          newSeg = newIt.next();
        }
        add.add(newSeg);
        newSeg = null;
      }
      
      // update index
      synchronized (index) {
        // remove first otherwise we get in trouble
        removeAll(rem);
        addAll(add);
      }
      
      //System.out.println("INDEX UPDATE: rem: " + rem + " add: " + add);
    }
  }
  
  public synchronized boolean isComplete() {
    // don't use equals() with the magic triple...
    Triple prev = null;
    for (SegmentInfo seg : index.values()) {
      if ((null==prev && !seg.getLowTriple().isMagic())
          || (null != prev && !seg.getLowTriple().equals(prev))) {
        return false;
      }
      prev = seg.getHighTriple();
    }
    return prev.isMagic();
  }
  
  synchronized SegmentInfo[] toArray() {
    return index.values().toArray(new SegmentInfo[index.size()]);
  }
  
  synchronized SegmentInfo[] toArray(Triple pattern) {
    SortedMap<Triple, SegmentInfo> index = this.index.tailMap(locate(pattern).getLowTriple());
    Comparator cmp = ((MagicComparator) index.comparator()).getBase();
    ArrayList<SegmentInfo> segments = new ArrayList<SegmentInfo>();
    boolean first = true;
    for (SegmentInfo segment : index.values()) {
      if (first || cmp.match(segment.getLowTriple(), pattern)) {
        segments.add(segment);
        first = false;
      } else {
        break;
      }
    }
    return segments.toArray(new SegmentInfo[segments.size()]);
  }
  
  synchronized SegmentInfo[] toArray(Triple start, Triple end) {
    return index.navigableKeySet().subSet(start, end).toArray(new SegmentInfo[]{});
  }
  
  public synchronized int size() {
    return index.size();
  }
  
  @Override
  public synchronized String toString() {
    StringBuilder str = new StringBuilder("SegmentIndex(");
    for (SegmentInfo info : index.values()) {
      str.append(info.getSegmentId() + ":" + info.getLowTriple() + " ");
    }
    str.append(")");
    return str.toString();
  }
  
}
