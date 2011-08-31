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
import java.nio.ByteBuffer;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.TripleDeserializer;
import de.hpi.fgis.hdrs.TripleSerializer;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.tio.OrderedTripleSink;
import de.hpi.fgis.hdrs.tio.OrderedTripleSource;
import de.hpi.fgis.hdrs.tio.TripleScanner;

public class OrderedTriplesWritable extends CompressedWritable implements
    OrderedTripleSource {

  private Triple.COLLATION order = null;
  private ByteBuffer buffer = null;
  
  
  public OrderedTriplesWritable() {}
  
  
  public OrderedTriplesWritable(Triple.COLLATION order, int bufferSize) {
    this.order = order;
    buffer = ByteBuffer.allocate(1 + bufferSize);
    buffer.put(order.getCode());
  }
  
  public OrderedTriplesWritable(Triple.COLLATION order, Triple t) throws IOException {
    this(order, 1 + t.serializedSize());
    getWriter().add(t);
  }
  
  @Override
  public void read(ByteBuffer in) throws IOException {
    order = Triple.COLLATION.decode(in.get());
    buffer = in.slice();
  }

  @Override
  public ByteBuffer write() throws IOException {
    buffer.flip();
    return buffer;
  }
  
  @Override
  public COLLATION getOrder() {
    return order;
  }

  @Override
  public TripleScanner getScanner() throws IOException {
    return new TripleDeserializer(order, buffer);
  }
  
  public OrderedTripleSink getWriter() {
    return new TripleSerializer(order, buffer);
  }

}
