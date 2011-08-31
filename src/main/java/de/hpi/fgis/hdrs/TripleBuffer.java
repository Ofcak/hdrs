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

import java.nio.ByteBuffer;

import de.hpi.fgis.hdrs.Triple.COLLATION;

public class TripleBuffer extends TripleSerializer {

  public TripleBuffer(COLLATION order, ByteBuffer buf) {
    super(order, buf);
  }

  @Override
  public boolean add(Triple t) {
    while (!super.add(t)) {
      ByteBuffer newbuf = ByteBuffer.allocate(buf.capacity() << 1);
      System.arraycopy(buf.array(), buf.arrayOffset(), 
          newbuf.array(), newbuf.arrayOffset(), buf.capacity());
      newbuf.position(newbuf.arrayOffset() + buf.position() - buf.arrayOffset());
      buf = newbuf;
    }
    return true;
  }
  
  public ByteBuffer getBuffer() {
    return buf;
  }
}
