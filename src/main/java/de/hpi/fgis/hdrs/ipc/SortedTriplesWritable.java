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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.TripleDeserializer;
import de.hpi.fgis.hdrs.TripleSerializer;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.tio.IteratorScanner;
import de.hpi.fgis.hdrs.tio.OrderedTripleSource;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.tio.TripleSink;
import de.hpi.fgis.hdrs.tio.TripleSourceWithSize;

public class SortedTriplesWritable extends CompressedBlockedWritable
implements TripleSink, OrderedTripleSource, TripleSourceWithSize {

  final private Triples triples;
  
  public SortedTriplesWritable() {
    triples = new TripleBuffer();
  }
  
  public SortedTriplesWritable(Triple.COLLATION order) {
    triples = new TripleTree(order);
  }
  
  @Override
  public void read(BlockCompressorInput in) throws IOException {
    triples.read(in);
  }

  @Override
  public void write(BlockCompressorOutput out) throws IOException {
    triples.write(out);
  }

  @Override
  public boolean add(Triple triple) throws IOException {
    return triples.add(triple);
  }

  @Override
  public COLLATION getOrder() {
    return triples.getOrder();
  }

  @Override
  public TripleScanner getScanner() throws IOException {
    return triples.getScanner();
  }
  
  @Override
  public int getSizeBytes() {
    return triples.getSizeBytes();
  }
  
  public int getNumberOfTriples() {
    return triples.getNumberOfTriples();
  }
  
  
  
  private static interface Triples 
  extends TripleSink, OrderedTripleSource, TripleSourceWithSize{
    
    void write(BlockCompressorOutput out) throws IOException;
    
    void read(BlockCompressorInput in) throws IOException;
    
    int getNumberOfTriples();
    
  }
  
  private static class TripleTree implements Triples {

    final private SortedMap<Triple, Triple> triples;
    private int size = 0;
    
    private TripleTree(Triple.COLLATION order) {
      triples = new TreeMap<Triple, Triple>(order.comparator());
    }
    
    @Override
    public boolean add(Triple triple) throws IOException {
      Triple old = triples.put(triple, triple);
      if (null != old) {
        triple.merge(old);
        return false;
      }
      size += 1 + triple.serializedSize();
      return true;
    }

    @Override
    public COLLATION getOrder() {
      return ((Triple.Comparator) triples.comparator()).getCollation();
    }

    @Override
    public TripleScanner getScanner() throws IOException {
      return new IteratorScanner(triples.values().iterator());
    }

    @Override
    public void write(BlockCompressorOutput out) throws IOException {
      ByteBuffer buf = ByteBuffer.allocate(64 * 1024);
      buf.put(getOrder().getCode());
      
      boolean firstBuffer = true;
      Iterator<Triple> it = triples.values().iterator();
      Triple t = null;
      while (it.hasNext()) {
        TripleSerializer serializer = new TripleSerializer(getOrder(), buf);
        if (null != t) {
          if (!serializer.add(t)) {
            // triple is larger than block size
            ByteBuffer largeBuf = ByteBuffer.allocate( t.serializedSize()
                + (firstBuffer ? 2 : 1));
            if (firstBuffer) {
              largeBuf.put(getOrder().getCode());
              buf.clear();
            }
            TripleSerializer largeSerializer = new TripleSerializer(getOrder(), largeBuf);
            if (!largeSerializer.add(t)) {
              throw new IOException("triple too large : " + t.serializedSize());
            }
            largeBuf.flip();
            out.write(largeBuf);
          }
        }
        firstBuffer = false;
        
        while (it.hasNext()) {
          t = it.next();
          if (!serializer.add(t)) { 
            break;
          }
        }
        buf.flip();
        out.write(buf);
        buf.clear();
      }
    } 

    @Override
    public void read(BlockCompressorInput in) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getSizeBytes() {
      return size;
    }

    @Override
    public int getNumberOfTriples() {
      return triples.size();
    }
    
  }
  
  private static class TripleBuffer implements Triples {
    
    private List<ByteBuffer> blocks = new ArrayList<ByteBuffer>();
    private Triple.COLLATION order = null;

    @Override
    public boolean add(Triple triple) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public COLLATION getOrder() {
      return order;
    }

    @Override
    public TripleScanner getScanner() throws IOException {
      return new TripleScanner() {
        TripleDeserializer deserializer;
        Iterator<ByteBuffer> buffers = blocks.iterator();

        @Override
        public COLLATION getOrder() {
          return order;
        }

        @Override
        protected Triple nextInternal() throws IOException {
          while (null == deserializer || !deserializer.next()) {
            if (!buffers.hasNext()) {
              return null;
            }
            deserializer = new TripleDeserializer(order, buffers.next());
          }
          return deserializer.pop();
        }

        @Override
        public void close() throws IOException {
          
        }
        
      };
            
    }

    @Override
    public void read(BlockCompressorInput in) throws IOException {
      ByteBuffer block;
      while (null != (block = in.read())) {
        if (null == order) {
          order = Triple.COLLATION.decode(block.get());
          blocks.add(block.slice());
        } else {
          blocks.add(block);
        }
      }
    }

    @Override
    public void write(BlockCompressorOutput out) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getSizeBytes() {
      int size = 0;
      for (ByteBuffer buf : blocks) {
        size += buf.limit();
      }
      return size -1;
    }

    @Override
    public int getNumberOfTriples() {
      // there's no way to tell.
      return -1;
    }
    
  }

  
  

}
