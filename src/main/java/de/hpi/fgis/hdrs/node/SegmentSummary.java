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

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.ipc.Writable;
import de.hpi.fgis.hdrs.node.Node.SegmentStatus;
import de.hpi.fgis.hdrs.routing.SegmentInfo;
import de.hpi.fgis.hdrs.segment.Segment.SegmentSize;
import de.hpi.fgis.hdrs.triplefile.TripleFileInfo;

public class SegmentSummary implements Writable {

  private SegmentInfo info;
  private SegmentStatus status;
  private SegmentSize size = null;
  private int peerId;
  private Triple.COLLATION order;
  private boolean compacting;
  private TripleFileInfo[] fileInfo;
  
  public SegmentSummary() {}
  
  public SegmentSummary(SegmentInfo info, SegmentStatus status, SegmentSize size,
      int peerId, Triple.COLLATION order, boolean compacting, TripleFileInfo[] fileInfo) {
    this.info = info;
    this.status = status;
    this.size = size;
    this.peerId = peerId;
    this.order = order;
    this.compacting = compacting;
    this.fileInfo = fileInfo;
  }
  
  public SegmentSummary(SegmentInfo info, int peerId, Triple.COLLATION order) {
    this(info, null, null, peerId, order, false, null);
  }

  public SegmentInfo getInfo() {
    return info;
  }
  
  public SegmentStatus getStatus() {
    return status;
  }
  
  public SegmentSize getSize() {
    return size;
  }
  
  public int getPeerId() {
    return peerId;
  }
  
  public Triple.COLLATION getIndex() {
    return order;
  }
  
  public boolean getCompacting() {
    return compacting;
  }
  
  public TripleFileInfo[] getFileInfo() {
    return fileInfo;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    info = new SegmentInfo();
    info.readFields(in);
    int statusOrd = in.readInt();
    status = -1 == statusOrd ? null : SegmentStatus.values()[statusOrd];
    if (in.readBoolean()) {
      size = new SegmentSize();
      size.readFields(in);
    }
    peerId = in.readInt();
    order = Triple.COLLATION.decode(in.readByte());
    compacting = in.readBoolean();
    int fileInfoSize = in.readInt();
    if (0 < fileInfoSize) {
      fileInfo = new TripleFileInfo[fileInfoSize];
      for (int i=0; i<fileInfoSize; ++i) {
        fileInfo[i] = new TripleFileInfo();
        fileInfo[i].readFields(in);
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    info.write(out);
    out.writeInt(null == status ? -1 : status.ordinal());
    if (null == size) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      size.write(out);
    }
    out.writeInt(peerId);
    out.write(order.getCode());
    out.writeBoolean(compacting);
    if (null == fileInfo) {
      out.writeInt(0);
    } else {
      out.writeInt(fileInfo.length);
      for (int i=0; i<fileInfo.length; ++i) {
        fileInfo[i].write(out);
      }
    }
  }

}
