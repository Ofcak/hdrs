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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.routing.SegmentInfo;


/**
 * An ordered list of segment descriptors that represents all segments
 * online at a specific node.
 * 
 * @author daniel.hefenbrock
 *
 */
public class SegmentCatalog implements Writable {

  private long version = 0;
  private final Map<Triple.COLLATION, List<SegmentInfo>> lists;
  
  // ctor for rpc
  public SegmentCatalog() {
    lists = new EnumMap<Triple.COLLATION, List<SegmentInfo>>(Triple.COLLATION.class);
  }
  
  // ctor for node
  public SegmentCatalog(long version, Map<Triple.COLLATION, List<SegmentInfo>> lists) {
    this.version = version;
    this.lists = lists;
  }
  
  public long getVersion() {
    return version;
  }
  
  public List<SegmentInfo> getSegments(Triple.COLLATION order) {
    return lists.get(order);
  }

  
  public static SegmentCatalog read(DataInput in) throws IOException {
    SegmentCatalog catalog = new SegmentCatalog();
    catalog.readFields(in);
    return catalog;
  }
  
  
  @Override
  public void readFields(DataInput in) throws IOException {
    version = in.readLong();
    for (Triple.COLLATION order : Triple.COLLATION.values()) {
      int size = in.readInt();
      if (-1 == size) {
        continue;
      }
      List<SegmentInfo> list = new ArrayList<SegmentInfo>(size);
      for (int i=0; i<size; ++i) {
        list.add(SegmentInfo.read(in));
      }
      lists.put(order, list);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(version);
    for (Triple.COLLATION order : Triple.COLLATION.values()) {
      List<SegmentInfo> list = lists.get(order);
      if (null == list) {
        out.writeInt(-1);
        continue;
      }
      out.writeInt(list.size());
      for (SegmentInfo info : list) {
        info.write(out);
      }
    }
  }
  
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder("SegmentCatalog(version = ");
    str.append(version);
    for (Triple.COLLATION order : lists.keySet()) {
      str.append(", " + order + " = " + lists.get(order).size() + " segments");
    }
    str.append(")");
    return str.toString();
  }
  
}
