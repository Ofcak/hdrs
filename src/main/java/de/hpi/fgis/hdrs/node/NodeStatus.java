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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import de.hpi.fgis.hdrs.ipc.Writable;

public class NodeStatus implements Writable {

  long heapspace;
  long segmentBuffer;
  int transactionBuffer;
  
  int nSegments;
  int nTransactions;
  
  
  public NodeStatus() {}
  
  public NodeStatus(long heapspace, long segmentBuffer, int transactionBuffer,
      int nSegments, int nTransactions) {
    this.heapspace = heapspace;
    this.segmentBuffer = segmentBuffer;
    this.transactionBuffer = transactionBuffer;
    this.nSegments = nSegments;
    this.nTransactions = nTransactions;
  }
  
  public long getHeapSpace() {
    return heapspace;
  }
  
  public long getSegmentBuffer() {
    return segmentBuffer;
  }
  
  public int getTransactionBuffer() {
    return transactionBuffer;
  }

  public int getNumberOfSegments() {
    return nSegments;
  }

  public int getNumberOfTransactions() {
    return nTransactions;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    heapspace = in.readLong();
    segmentBuffer = in.readLong();
    transactionBuffer = in.readInt();
    nSegments = in.readInt();
    nTransactions = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(heapspace);
    out.writeLong(segmentBuffer);
    out.writeInt(transactionBuffer);
    out.writeInt(nSegments);
    out.writeInt(nTransactions);
  }

  

}
