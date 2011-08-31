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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.tio.MultiSourceScanner;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.tio.TripleSourceWithSize;

/**
 * 
 * @author daniel.hefenbrock
 *
 */
public class MultiSegmentWrite implements Writable, TripleSourceWithSize {

  final private Map<Long, SortedTriplesWritable> segments = 
    new HashMap<Long, SortedTriplesWritable>();
    
  public MultiSegmentWrite() {}
  
  
  public boolean add(long segmentId, Triple.COLLATION order, Triple t) throws IOException {
    SortedTriplesWritable triples = segments.get(Long.valueOf(segmentId));
    if (null == triples) {
      triples = new SortedTriplesWritable(order);
      segments.put(Long.valueOf(segmentId), triples);
    }
    if (triples.add(t)) {
      return true;
    }
    return false;
  }
  
  
  @Override
  public TripleScanner getScanner() throws IOException {
    return new MultiSourceScanner(segments.values());
  }
  
  
  public TripleSourceWithSize getTriples(long segmentId) {
    return segments.get(Long.valueOf(segmentId));
  }
  
  
  public Set<Long> getSegments() {
    return segments.keySet();
  }
  
  
  @Override
  public int getSizeBytes() {
    int size = 0;
    for (SortedTriplesWritable write : segments.values()) {
      size += write.getSizeBytes();
    }
    return size;
  }
  
  
  @Override
  public void readFields(DataInput in) throws IOException {
    int numSegments = in.readInt();
    for (int s=0; s<numSegments; ++s) {
      long segmentId = in.readLong();
      SortedTriplesWritable triples = new SortedTriplesWritable();
      triples.readFields(in);
      segments.put(Long.valueOf(segmentId), triples);
    }
  }


  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(segments.size());
    for (Map.Entry<Long, SortedTriplesWritable> segment : segments.entrySet()) {
      out.writeLong(segment.getKey().longValue());
      segment.getValue().write(out);
    }
  }
  
  
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder("MultiSegmentWrite(");
    for (Map.Entry<Long, SortedTriplesWritable> entry : segments.entrySet()) {
      str.append(entry.getKey() + " = " 
          + entry.getValue().getNumberOfTriples() + "triples, "
          + entry.getValue().getSizeBytes() + " bytes ");
    }
    str.append(")");
    return str.toString();
  }


}
