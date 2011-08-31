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

package de.hpi.fgis.hdrs.node;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.ipc.RPC;

import de.hpi.fgis.hdrs.Configuration;
import de.hpi.fgis.hdrs.Peer;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.ipc.NodeProtocol;

public class NodeLauncher {

  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException  {
    if (4 != args.length) {
      throw new IOException("start/stop/kill, index file, peer file, or node id not specified");
    }
    
    Set<Triple.COLLATION> indexes = Configuration.readIndexes(args[1]);
    ArrayList<Peer> peers = Configuration.readPeers(args[2]);
    int nodeId;
    try {
      nodeId = Integer.parseInt(args[3]);
    } catch (NumberFormatException ex) {
      throw new IOException("node id is not an integer");
    }
    Peer thisNode = null;
    for (Peer peer : peers) {
      if (peer.getId() == nodeId) {
        thisNode = peer;
        break;
      }
    }
    if (null == thisNode) {
      throw new IOException("local node (id " + nodeId + ") not in peer file");
    }
    
    Configuration conf = Configuration.create(thisNode.getId());
    if ("start".equals(args[0])) {
      Node node = new Node(conf, indexes, peers, thisNode);
      Thread thread = new Thread(node);
      thread.start();
    } else if ("stop".equals(args[0])) { // stop
      NodeProtocol node = thisNode.getProxy(conf);
      node.shutDown();
      RPC.stopProxy(node);
    } else if ("kill".equals(args[0])) {
      NodeProtocol node = thisNode.getProxy(conf);
      node.kill();
      RPC.stopProxy(node);
    } else {
      throw new IOException("neither start, stop, nor kill specified");
    }
  }
  

  /**
   * Create a local node with no peers for testing.
   * @return
   * @throws IOException
   */
  public static Node createTmpNode(Configuration conf, int port,
      Triple.COLLATION...indexes) throws IOException {
    File dir = File.createTempFile("hdrs_node", "");
    if (!dir.delete() || !dir.mkdir()) {
      throw new IOException("Couldn't create tmp dir for test node.");
    }
    dir.deleteOnExit();
    conf.set(Configuration.KEY_ROOT_DIR, dir.getAbsolutePath());
    Peer local = new Peer(1, "localhost", port);
    ArrayList<Peer> peers = new ArrayList<Peer>();
    peers.add(local);
    Set<Triple.COLLATION> index = Triple.COLLATION.allOf(indexes);
    return new Node(conf, index, peers, local);
  }
}
