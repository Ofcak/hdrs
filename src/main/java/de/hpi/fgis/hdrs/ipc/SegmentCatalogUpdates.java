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


/**
 * Holds a bunch of segment catalog updates
 * @author daniel.hefenbrock
 *
 */
public class SegmentCatalogUpdates implements Writable {

  private final Map<Integer, SegmentCatalog> updates;
  
  public SegmentCatalogUpdates() {
    updates = new HashMap<Integer, SegmentCatalog>();
  }
  
  
  public void add(int peerId, SegmentCatalog catalog) {
    updates.put(peerId, catalog);
  }
  
  
  public Map<Integer, SegmentCatalog> getUpdates() {
    return updates;
  }
  

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    for (int i=0; i<size; ++i) {
      int peerId = in.readInt();
      SegmentCatalog catalog = SegmentCatalog.read(in);
      updates.put(peerId, catalog);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(updates.size());
    for (Map.Entry<Integer, SegmentCatalog> update : updates.entrySet()) {
      out.writeInt(update.getKey());
      update.getValue().write(out);
    }
  }
  
  
}
