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

package de.hpi.fgis.hdrs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import de.hpi.fgis.hdrs.ipc.Writable;

public class StoreInfo implements Writable {

  
  Set<Triple.COLLATION> indexes;
  ArrayList<Peer> peers;
  
  int clientId;

  
  public StoreInfo() {}
  
  public StoreInfo(Set<Triple.COLLATION> indexes, ArrayList<Peer> peers, int clientId) {
    this.indexes = indexes;
    this.peers = peers;
    this.clientId = clientId;
  }
  
  public Set<Triple.COLLATION> getIndexes() {
    return indexes;
  }
  
  public ArrayList<Peer> getPeers() {
    return peers;
  }
  
  public int getClientId() {
    return clientId;
  }
  
  
  @Override
  public void readFields(DataInput in) throws IOException {
    int n = in.readInt();
    indexes = Triple.COLLATION.allOf();
    for (int i=0; i<n; ++i) {
      indexes.add(Triple.COLLATION.decode(in.readByte()));
    }
    n = in.readInt();
    peers = new ArrayList<Peer>(n);
    for (int i=0; i<n; ++i) {
      Peer peer = new Peer();
      peer.readFields(in);
      peers.add(peer);
    }
    clientId = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(indexes.size());
    for (Triple.COLLATION index : indexes) {
      out.write(index.getCode());
    }
    out.writeInt(peers.size());
    for (Peer peer : peers) {
      peer.write(out);
    }
    out.writeInt(clientId);
  }
  
  
}
