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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

import de.hpi.fgis.hdrs.ipc.NodeProtocol;
import de.hpi.fgis.hdrs.ipc.ProtocolVersion;
import de.hpi.fgis.hdrs.ipc.SegmentCatalog;
import de.hpi.fgis.hdrs.ipc.Writable;

/**
 * A remote node.
 * 
 * @author daniel.hefenbrock
 *
 */
public class Peer implements Writable, Comparable<Peer> {

  private int id;
  private String address;
  private int port;
  
  private SegmentCatalog catalog = null;
  
  public Peer() {}
  
  public Peer(int id, String address, int port) {
    this.id = id;
    this.address = address;
    this.port = port;
  }
  
  // copy ctor
  public Peer(Peer peer) {
    this.id = peer.id;
    this.address = peer.address;
    this.port = peer.port;
  }
  
  public int getId() {
    return id;
  }
  
  public String getAddress() {
    return address;
  }
  
  public String getHostName() {
    try {
      return InetAddress.getByName(address).getHostName();
    } catch (UnknownHostException e) {
      return address;
    }
  }
  
  public int getPort() {
    return port;
  }
  
  
  public SegmentCatalog getSegmentCatalog() {
    return catalog;
  }
  
  
  public void setSegmentCatalog(SegmentCatalog catalog) {
    this.catalog = catalog;
  }
  
  
  public NodeProtocol getProxy(Configuration conf) throws IOException {
    InetSocketAddress addr = getConnectAddress();
    NodeProtocol proxy = getNodeProxy(conf, addr);
    return proxy;
  }
  
  
  public static NodeProtocol getNodeProxy(Configuration conf, InetSocketAddress addr) 
  throws IOException {
    return (NodeProtocol) RPC.getProxy(
        NodeProtocol.class, ProtocolVersion.versionID, addr, conf);
  }
  
  
  public InetSocketAddress getConnectAddress() {
    return NetUtils.createSocketAddr(address, port);
  }
  
  
  @Override
  public String toString() {
    return String.format("%d (%s:%d)", id, address, port);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readInt();
    address = in.readLine();
    port = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(id);
    out.writeBytes(address);
    out.write('\n');
    out.writeInt(port);
  }

  @Override
  public int compareTo(Peer o) {
    return getId() - o.getId();
  }
  
}
