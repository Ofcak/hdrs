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
import de.hpi.fgis.hdrs.tio.TripleScanner;

public class TripleDeserializer extends TripleScanner {

  private final Triple.COLLATION order;
  private final ByteBuffer buf;
  
  private int p1Off = 0;
  private int p1Len = 0;
  
  private int p2Off = 0;
  private int p2Len = 0;
  
  private int p3Off = 0;
  private int p3Len = 0;
  
  int multiplicity = 0;
  
  public TripleDeserializer(Triple.COLLATION order, ByteBuffer buf) {
    this.order = order;
    this.buf = buf;
  }
  
  @Override
  public COLLATION getOrder() {
    return order;
  }

  @Override
  protected Triple nextInternal() throws IOException {
    if (!buf.hasRemaining()) {
      return null;
    }
    byte code = buf.get();
    multiplicity = buf.getInt();
    switch (code) {
    case TripleSerializer.NEW_P1:
      p1Len = order.readPos1Len(buf);
      p1Off = buf.position();
      buf.position(p1Off + p1Len);
    case TripleSerializer.NEW_P2: 
      p2Len = order.readPos2Len(buf);
      p2Off = buf.position();
      buf.position(p2Off + p2Len);
    case TripleSerializer.NEW_P3:
      p3Len = order.readPos3Len(buf);
      p3Off = buf.position();
      buf.position(p3Off + p3Len);
      break;
    default: throw new IOException("invalid triple encoding");
    }
    return makeTriple();
  }
  
  private Triple makeTriple() {
    return order.newInstance(
        buf.array(), 
        buf.arrayOffset() + p1Off, p1Len, 
        buf.arrayOffset() + p2Off, p2Len, 
        buf.arrayOffset() + p3Off, p3Len, multiplicity);
  }

  @Override
  public void close() throws IOException {
  }
  
  public ByteBuffer slice() throws IOException {
    int len = p1Len + p2Len + p3Len + 1 + Triple.HEADER_SIZE;
    if (buf.position() < len) {
      return null;
    }
    Triple t = makeTriple();
    t = t.copy();
    buf.position(buf.position() - len);
    ByteBuffer buf = this.buf.slice();
    TripleSerializer serializer = new TripleSerializer(order, buf);
    serializer.add(t);
    buf.rewind();
    return buf;
  }
  
  
}
