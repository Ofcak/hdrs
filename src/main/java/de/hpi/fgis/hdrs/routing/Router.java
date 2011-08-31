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

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;

import de.hpi.fgis.hdrs.Configuration;
import de.hpi.fgis.hdrs.Peer;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.ipc.NodeProtocol;
import de.hpi.fgis.hdrs.ipc.SegmentCatalog;
import de.hpi.fgis.hdrs.ipc.SegmentCatalogUpdates;
import de.hpi.fgis.hdrs.node.Node;

public class Router {

  static final Log LOG = LogFactory.getLog(Router.class);
  
  final Configuration conf;
  
  final Node node;
  
  /**
   * Peer representing this node.  Also in peers.
   */
  final Peer localPeer;
  
  
  volatile boolean syncLocalCatalog = true;
  
  /**
   * Peers of this node.  Also contains this node.
   * The content and order of this array MUST be the same at all nodes.
   */
  final ArrayList<Peer> peers;
  
  /**
   * Ordered collections of all segments presents in each index.
   */
  final Map<Triple.COLLATION, SegmentIndex> index =
    new EnumMap<Triple.COLLATION, SegmentIndex>(Triple.COLLATION.class);
  
  
  private final Random random = new Random(System.currentTimeMillis());
  
  
  public Router(Configuration conf, ArrayList<Peer> peers) {
    this(conf, peers, null, null);
  }
  
  
  public Router(Configuration conf, ArrayList<Peer> peers, Peer localPeer, Node node) {
    this.conf = conf;
    this.peers = peers;
    this.localPeer = localPeer;
    this.node = node;
    if (null != localPeer) {
      localPeer.setSegmentCatalog(new SegmentCatalog());
    }
  }
  
  
  public Configuration getConf() {
    return conf;
  }
  
  
  public ArrayList<Peer> getPeers() {
    return peers;
  }
  
  
  public void initialize(Triple.COLLATION order) {
    index.put(order, new SegmentIndex(order));
  }
  
  
  public Peer getLocalPeer() {
    return localPeer;
  }
  
  
  public Set<Triple.COLLATION> getCollations() {
    return index.keySet();
  }
  
  
  public SegmentCatalog getLocalCatalog() {
    return localPeer.getSegmentCatalog();
  }
  
  
  public int getNumberOfSegments() {
    int size = 0;
    for (SegmentIndex index : this.index.values()) {
      size += index.size();
    }
    return size;
  }
  
  public SegmentInfo[] getIndex(Triple.COLLATION order) {
    SegmentIndex idx = index.get(order);
    if (null == idx) {
      return null;
    }
    return idx.toArray();
  }
  
  public SegmentInfo[] getIndex(Triple.COLLATION order, Triple pattern) {
    SegmentIndex idx = index.get(order);
    if (null == idx) {
      return null;
    }
    return idx.toArray(pattern);
  }
  
  public SegmentInfo[] getIndexRange(Triple.COLLATION order, Triple start, Triple stop) {
    SegmentIndex idx = index.get(order);
    if (null == idx) {
      return null;
    }
    return idx.toArray(start, stop);
  }
  
  public PeerView[] getView() {
    PeerView[] view = new PeerView[peers.size()];
    int i = 0;
    for (Peer peer : peers) {
      view[i++] = new PeerView(peer.getId(), 
          null==peer.getSegmentCatalog() ? -1 : peer.getSegmentCatalog().getVersion());
    }
    return view;
  }
  
  // TODO this is just a temporary solution
  public Peer getPeerById(int id) {
    for (Peer peer : peers) {
      if (peer.getId() == id) {
        return peer;
      }
    }
    return null;
  }
  
  public Triple.COLLATION getIndex(SegmentInfo info) {
    for (Map.Entry<Triple.COLLATION, SegmentIndex> entry : index.entrySet()) {
      if (entry.getValue().contains(info)) {
        return entry.getKey();
      }
    }
    return null;
  }
  
  public Peer locateSegment(SegmentInfo info) {
    Triple.COLLATION index = getIndex(info);
    if (null == index) {
      return null;
    }
    return locateSegment(index, info);
  }
  
  public Peer locateSegment(Triple.COLLATION index, SegmentInfo info) {
    // shift hash value by 0..5 depending on the index
    int hash = Math.abs(((int) index.getCode()) - 1 + info.getLowTriple().hashCode());
    return peers.get(hash % peers.size());
  }

  public SegmentInfo locateTriple(COLLATION order, Triple t) {
    return index.get(order).locate(t);
  }
  
  
  public SegmentCatalogUpdates antiEntropy(long[] versionVector) throws IOException {
    // sanity check
    if (versionVector.length != peers.size()) {
      throw new IOException("wrong version vector length");
    }
    
    // check if we need to update the local catalog.
    syncLocalCatalog();
    
    Peer runAntiEntropy = null;
    
    SegmentCatalogUpdates updates = new SegmentCatalogUpdates();
    
    for (int i=0; i<peers.size(); ++i) {
      SegmentCatalog catalog = peers.get(i).getSegmentCatalog();
      if (null != catalog
          && catalog.getVersion() > versionVector[i]) {
        // version we have is newer
        updates.add(peers.get(i).getId(), catalog);
      } else if (null == catalog
          || catalog.getVersion() < versionVector[i]) {
        // our version is older, need update
        runAntiEntropy = peers.get(i); // TODO what if several?
      }
    }
    
    //LOG.info("Anti-entropy: sending " + updates.getUpdates().size() + " updates.");
    
    if (null != runAntiEntropy) {
      node.runAntiEntropy(runAntiEntropy);
    }
    
    return updates;
  }
  
  
  public void syncLocalCatalog() {
    if (null != node) {
      synchronized (this) {
        if (syncLocalCatalog) {
          long currentVersion = localPeer.getSegmentCatalog().getVersion();
          SegmentCatalog catalog = node.getLocalSegmentCatalog(currentVersion+1);
          syncLocalCatalog = false;
          processSegmentCatalogUpdate(localPeer, catalog);
          LOG.info("Local segment catalog updated.");
        }
      }
    }
  }
  
  public synchronized void localCatalogUpdate() {
    syncLocalCatalog = true;
  }
  
  
  public boolean checkComplete() {
    for (SegmentIndex i : index.values()) {
      if (!i.isComplete()) {
        return false;
      }
    }
    return true;
  }
  
  
  public boolean update(Peer hint) throws IOException {
    syncLocalCatalog();
    if (1 == peers.size() && null != localPeer) {
      // only this node, do nothing.
      return false;
    }
    Peer peer = hint;
    while (localPeer == peer || null == peer) {
      peer = peers.get(random.nextInt(peers.size()));
    }
    
    //LOG.info("Running anti-entropy with peer " + peer);
    
    // construct version vector.
    long[] versionVector = new long[peers.size()];
    for (int i=0; i<peers.size(); ++i) {
      SegmentCatalog catalog = peers.get(i).getSegmentCatalog();
      versionVector[i] = null == catalog ? 0 : catalog.getVersion();
    }
    
    NodeProtocol proxy = peer.getProxy(conf);
    SegmentCatalogUpdates updates = proxy.antiEntropy(versionVector);
    RPC.stopProxy(proxy);
    
    if (!updates.getUpdates().isEmpty()) {
      processSegmentCatalogUpdates(updates);
      LOG.info("Anti-Entropy: Received " + updates.getUpdates().size() + " updates from peer "
          + peer);
      return true;
    }
    return false;
  }
  
  
  private synchronized void processSegmentCatalogUpdates(SegmentCatalogUpdates updates) {
    for (int peerId : updates.getUpdates().keySet()) {
      Peer peer = getPeerById(peerId);
      processSegmentCatalogUpdate(peer, updates.getUpdates().get(peerId));
    }
  }
  
  
  private synchronized void processSegmentCatalogUpdate(Peer peer, SegmentCatalog newCatalog) {
  
    SegmentCatalog oldCatalog = peer.getSegmentCatalog();     
    if (oldCatalog != null &&
        oldCatalog.getVersion() >= newCatalog.getVersion()) {
      return;
    }
            
    // update peer catalog
    peer.setSegmentCatalog(newCatalog);
      
    // update segment indexes
    for (Triple.COLLATION order : index.keySet()) {
      List<SegmentInfo> oldList = null == oldCatalog ? null: oldCatalog.getSegments(order);
      List<SegmentInfo> newList = newCatalog.getSegments(order);
      index.get(order).update(oldList, newList);
    }
  }
  
}