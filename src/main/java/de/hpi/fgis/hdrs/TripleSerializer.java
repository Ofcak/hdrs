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

import java.io.IOException;
import java.nio.ByteBuffer;

import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.tio.OrderedTripleSink;

public class TripleSerializer implements OrderedTripleSink {

  static final byte NEW_P1 = 1;
  static final byte NEW_P2 = 2;
  static final byte NEW_P3 = 3;
  
  protected ByteBuffer buf;
  private final Triple.COLLATION order;
  
  private byte[] p1Buf = null;
  private int p1Off = 0;
  private int p1Len = 0;
  
  private byte[] p2Buf = null;
  private int p2Off = 0;
  private int p2Len = 0;
  
  
  public TripleSerializer(Triple.COLLATION order, ByteBuffer buf) {
    this.order = order;
    this.buf = buf;
  }
  
  @Override
  public boolean add(Triple t) {
    if (null != p1Buf &&
        Triple.buffersEqual(p1Buf, p1Off, p1Len, 
            t.getBuffer(), order.getPos1Off(t), order.getPos1Len(t))) {
      if (Triple.buffersEqual(p2Buf, p2Off, p2Len, 
          t.getBuffer(), order.getPos2Off(t), order.getPos2Len(t))) {
        if (buf.remaining() < 1 + (2 * Integer.SIZE)/Byte.SIZE + order.getPos3Len(t)) {
          return false;
        }
        buf.put(NEW_P3);
        buf.putInt(t.getMultiplicity());
      } else {
        if (buf.remaining() < 1 + (3 * Integer.SIZE)/Byte.SIZE + 
            order.getPos3Len(t) + order.getPos2Len(t)) {
          return false;
        }
        buf.put(NEW_P2);
        buf.putInt(t.getMultiplicity());
        writePos2(t);
      }
    } else {
      if (buf.remaining() < 1 + t.serializedSize()) {
        return false;
      }
      buf.put(NEW_P1);
      buf.putInt(t.getMultiplicity());
      writePos1(t);
      writePos2(t);
    }
    order.writePos3Len(t, buf);
    buf.put(t.getBuffer(), order.getPos3Off(t), order.getPos3Len(t));
    return true;
  }
  
  
  private void writePos1(Triple t) {
    order.writePos1Len(t, buf);
    buf.put(t.getBuffer(), order.getPos1Off(t), order.getPos1Len(t));
    p1Buf = t.getBuffer();
    p1Off = order.getPos1Off(t);
    p1Len = order.getPos1Len(t);
  }
  
  private void writePos2(Triple t) {
    order.writePos2Len(t, buf);
    buf.put(t.getBuffer(), order.getPos2Off(t), order.getPos2Len(t));
    p2Buf = t.getBuffer();
    p2Off = order.getPos2Off(t);
    p2Len = order.getPos2Len(t);
  }
  
  
  @Override
  public COLLATION getOrder() {
    return order;
  }
}
