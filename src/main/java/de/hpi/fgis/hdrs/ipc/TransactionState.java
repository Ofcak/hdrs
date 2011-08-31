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

public class TransactionState implements Writable {

  private boolean aborted = false;
  private long segmentId = 0;
  
  public TransactionState() {}
  
  private TransactionState(boolean aborted, long segmentId) {
    this.aborted = aborted;
    this.segmentId = segmentId;
  }
  
  public final static TransactionState OK = new TransactionState();
  public final static TransactionState ABORTED = new TransactionState(true, 0);
  
  public static TransactionState aborted(long segmentId) {
    return new TransactionState(true, segmentId);
  }
  
  public boolean isOk() {
    return !aborted;
  }
  
  public long getSegmentId() {
    return segmentId;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    aborted = in.readBoolean();
    segmentId = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(aborted);
    out.writeLong(segmentId);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (aborted ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TransactionState other = (TransactionState) obj;
    if (aborted != other.aborted) {
      return false;
    }
    return true;
  }
  
  

}
