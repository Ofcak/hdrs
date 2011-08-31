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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.MagicComparator;
import de.hpi.fgis.hdrs.ipc.Writable;


public class SegmentInfo implements Writable {

  private long segmentId;
    
  /*
   * The segment described by this segment info contains 
   * the triple range [lowTriple, highTriple).
   */
  
  private Triple lowTriple = null;
  private Triple highTriple = null;
  
  public SegmentInfo() {
  }
  
  public SegmentInfo(long segmentId, Triple lTriple, Triple hTriple) {
    this.segmentId = segmentId;
    lowTriple = lTriple;
    highTriple = hTriple;
  }
  
  
  public long getSegmentId() {
    return segmentId;
  }
  
  
  public Triple getHighTriple() {
    return highTriple;
  }
  
  
  public Triple getLowTriple() {
    return lowTriple;
  }
  
  
  public static SegmentInfo read(DataInput in) throws IOException {
    SegmentInfo res = new SegmentInfo();
    res.readFields(in);
    return res;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    segmentId = in.readLong();
    highTriple = Triple.readTriple(in);
    lowTriple = Triple.readTriple(in);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(segmentId);
    Triple.writeTriple(out, highTriple);
    Triple.writeTriple(out, lowTriple);
  }


  @Override
  public int hashCode() {
//    final int prime = 31;
//    int result = 1;
//    result = prime * result
//        + ((highTriple.isMagic()) ? 0 : highTriple.hashCode());
//    return result;
    return (int)(segmentId^(segmentId>>>32));
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SegmentInfo other = (SegmentInfo) obj;
//    if (highTriple == null) {
//      if (other.highTriple != null) {
//        return false;
//      }
//    } else if (!highTriple.equals(other.highTriple)) {
//      return false;
//    }
//    return true;
    return other.getSegmentId() == getSegmentId();
  }
  
  
  @Override
  public String toString() {
    return "Segment(" + segmentId + ")";
  }
  
  
  public static Comparator<SegmentInfo> getComparator(Triple.COLLATION order) {
    return new SegmentInfoComparator(order);
  }
  
  private static class SegmentInfoComparator implements Comparator<SegmentInfo> {

    private final MagicComparator comparator;
    
    SegmentInfoComparator(Triple.COLLATION order) {
      comparator = order.magicComparator();
    }
    
    @Override
    public int compare(SegmentInfo info1, SegmentInfo info2) {
      return comparator.compare(info1.getLowTriple(), info2.getLowTriple());
    }
    
  }
  
  public static SegmentInfo getSeed(long id) {
    return new SegmentInfo(id, Triple.MAGIC_TRIPLE, Triple.MAGIC_TRIPLE);
  }
}
