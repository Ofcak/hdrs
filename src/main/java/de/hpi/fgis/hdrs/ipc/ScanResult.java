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

import de.hpi.fgis.hdrs.Triple;

public class ScanResult implements Writable {

  public static enum State {
    OPEN ((byte) 1), // scanner is open, more triples to fetch
    DONE ((byte) 2), // scanner is done, no more triples
    END ((byte) 3), // scanner is at segment end, possibly more triples in subsequent segment
    ABORTED ((byte) 4), // scanner was aborted due to a segment split, reseek required
    INVALID ((byte) 5); // segment is invalid (i.e., doesn't exist anymore)
    
    private final byte code;
    
    State(final byte c) {
      code = c;
    }
    
    public byte getCode() {
      return code;
    }
    
    public static State decode(final byte code) {
      if (OPEN.code == code) return OPEN;
      if (DONE.code == code) return DONE;
      if (END.code == code) return END;
      if (ABORTED.code == code) return ABORTED;
      if (INVALID.code == code) return INVALID;
      throw new IllegalArgumentException("Invalid scan result state encoding");
    }
  };
  
  private State state;
  private long scannerId = 0;
  private OrderedTriplesWritable triples = null;
  
  
  public static final ScanResult INVALID;
  public static final ScanResult EMPTY;
  
  static {
    INVALID = new ScanResult();
    INVALID.setInvalid();
    EMPTY = new ScanResult();
    EMPTY.setDone();
  }
  
  public ScanResult() {}
  
  public ScanResult(long scannerId, OrderedTriplesWritable triples) {
    state = State.OPEN;
    this.scannerId = scannerId;
    this.triples = triples;
  }
  
  public ScanResult(long scannerId, Triple t, Triple.COLLATION order) throws IOException {
    this(scannerId, new OrderedTriplesWritable(order, t));
  }
  
  public void setDone() {
    state = State.DONE;
  }
  
  public void setEnd() {
    state = State.END;
  }
  
  public void setAborted() {
    state = State.ABORTED;
  }
  
  public void setInvalid() {
    state = State.INVALID;
  }
  
  public boolean isClosed() {
    return State.OPEN != state;
  }
  
  public boolean isDone() {
    return State.DONE == state;
  }
  
  public boolean isEnd() {
    return State.END == state;
  }
  
  public boolean isAborted() {
    return State.ABORTED == state;
  }
  
  public boolean isInvalid() {
    return State.INVALID == state;
  }
  
  public long getScannerId() {
    return scannerId;
  }
  
  public Triple.COLLATION getOrder() {
    return null == triples ? null : triples.getOrder(); // TODO FIXME
  }
  
  public OrderedTriplesWritable getTriples() {
    return triples;
  }
  
  
  @Override
  public void readFields(DataInput in) throws IOException {
    state = State.decode(in.readByte());
    if (!isInvalid()) {
      scannerId = in.readLong();
      if (0 != scannerId) {
        triples = new OrderedTriplesWritable();
        triples.readFields(in);
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(state.getCode());
    if (!isInvalid()) {
      out.writeLong(scannerId);
      if (0 != scannerId) {
        triples.write(out);
      }
    }
  }

}
