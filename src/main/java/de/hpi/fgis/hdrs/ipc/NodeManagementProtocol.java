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

import java.io.IOException;

import de.hpi.fgis.hdrs.node.NodeStatus;
import de.hpi.fgis.hdrs.node.SegmentSummary;
import de.hpi.fgis.hdrs.routing.PeerView;

public interface NodeManagementProtocol extends ProtocolVersion {

  /**
   * Shut down this node.
   */
  void shutDown();
  
  
  /**
   * Kill node.  May lead to data corruption.
   */
  void kill();
  
 
  
  NodeStatus getNodeStatus();
  
  SegmentSummary[] getIndexSummary(byte collation) throws IOException;


  SegmentSummary[] getPeerSummary() throws IOException;
  
  
  SegmentSummary getSegmentSummary(long segmentId) throws IOException;
  
  
  PeerView[] getView();
  
  boolean openSegment(long segmentId) throws IOException;
  
  public boolean requestCompaction(long segmentId) throws IOException;
  
  public boolean requestFlush(long segmentId) throws IOException;
  
  public boolean requestSplit(long segmentId) throws IOException;
  
}
