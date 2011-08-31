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

import de.hpi.fgis.hdrs.ipc.Writable;

public class PeerView implements Writable {

  private int peerId;
  private long catalogVersion;
  
  public PeerView() {}
  
  public PeerView(int peerId, long catalogVersion) {
    this.peerId = peerId;
    this.catalogVersion = catalogVersion;
  }
  
  public int getPeerId() {
    return peerId;
  }
  
  public long getCatalogVersion() {
    return catalogVersion;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    peerId = in.readInt();
    catalogVersion = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(peerId);
    out.writeLong(catalogVersion);
  }

}
