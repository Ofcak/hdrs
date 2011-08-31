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
import java.util.List;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.tio.IteratorScanner;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.tio.TripleSink;
import de.hpi.fgis.hdrs.tio.TripleSourceWithSize;

/**
 * 
 * @author daniel.hefenbrock
 *
 */
public class TriplesWritable extends CompressedWritable 
implements TripleSourceWithSize, TripleSink {

  private Triples triples;
  
  
  public TriplesWritable() {}
  
  
  @Override
  public boolean add(Triple triple) throws IOException {
    if (null == triples) {
      triples = new TripleList();
    }
    return triples.add(triple);
  }
  
  
  @Override
  public TripleScanner getScanner() throws IOException {
    if (null == triples) {
      return null;
    }
    return triples.getScanner();
  }
  
  
  @Override
  public int getSizeBytes() {
    if (null == triples) {
      return 0;
    }
    return triples.getSizeBytes();
  }
  
  
  public int getNumberOfTriples() {
    if (null == triples) {
      return 0;
    }
    return triples.getNumberOfTriples();
  }
  
  
  /*
   * Serialization separates triple headers from data. This allows
   * for the headers buffer to be GC'd once the triples are instantiated. 
   * 
   * 
   * update: this is broken now that everything is decompressed form a single
   * buffer.  TODO  enable multiple buffers, each compressed separately.
   * 
   */
  
  
  @Override
  public void read(ByteBuffer in) throws IOException {
    // read headers
    int hsz = in.getInt();
    ByteBuffer headers = ByteBuffer.wrap(in.array(), in.position(), hsz).slice();
    in.position(in.position() + hsz);
    // read data
    int dsz = in.getInt();
    ByteBuffer data = ByteBuffer.wrap(in.array(), in.position(), dsz).slice();   
    in.position(in.position() + dsz);
    triples = new TripleBuffer(headers, data);    
  }


  @Override
  public ByteBuffer write() throws IOException {
    if (null == triples) {
      triples = new TripleList();
    }
    ByteBuffer out = ByteBuffer.allocate(2 * Integer.SIZE/Byte.SIZE +
        getSizeBytes());
    // write headers
    out.putInt(triples.getHeadersSize());
    TripleScanner scanner = triples.getScanner();
    while (scanner.next()) {
      Triple.writeTripleHeader(out, scanner.pop());
    }
    scanner.close();
    // write data
    out.putInt(triples.getDataSize());
    scanner = triples.getScanner();
    while (scanner.next()) {
      Triple.writeTripleData(out, scanner.pop());
    }
    scanner.close();
    return out;
  }

  
  
  private static interface Triples extends TripleSink, TripleSourceWithSize {
    
    int getDataSize();
    
    int getHeadersSize();
    
    int getNumberOfTriples();
    
  }
  
  
  private static class TripleList implements Triples {

    private final List<Triple> triples = new ArrayList<Triple>();
    private int dataSize = 0;
    
    @Override
    public boolean add(Triple triple) throws IOException {
      triples.add(triple);
      dataSize += triple.bufferSize();
      return true;
    }

    @Override
    public TripleScanner getScanner() throws IOException {
      return new IteratorScanner(triples.iterator());
    }

    @Override
    public int getDataSize() {
      return dataSize;
    }

    @Override
    public int getHeadersSize() {
      return triples.size() * Triple.HEADER_SIZE;
    }
    
    @Override
    public int getSizeBytes() {
      return getHeadersSize() + getDataSize();
    }

    @Override
    public int getNumberOfTriples() {
      return triples.size();
    }
  }
  
  
  // this class is a hack
  private static class TripleBuffer extends TripleScanner implements Triples {

    final ByteBuffer headers;
    final ByteBuffer data;
    
    TripleBuffer(ByteBuffer headers, ByteBuffer data) {
      this.headers = headers;
      this.data = data;
    }
    
    @Override
    public boolean add(Triple triple) throws IOException {
      throw new UnsupportedOperationException();
    }

    /**
     * NOTE: there can only be one scanner at a time for this
     * triple source.
     */
    @Override
    public TripleScanner getScanner() throws IOException {
      return this;
    }

    @Override
    public int getSizeBytes() {
      return getHeadersSize() + getDataSize();
    }
    
    @Override
    public int getHeadersSize() {
      return headers.capacity ();
    }
    
    @Override
    public int getDataSize() {
      return data.capacity ();
    }
    
    @Override
    public COLLATION getOrder() {
      return null;
    }

    @Override
    protected Triple nextInternal() throws IOException {
      if (0 < headers.remaining()) {
        return Triple.readTriple(headers, data);
      }
      return null;
    }

    @Override
    public void close() throws IOException {
      headers.rewind();
      data.rewind();
    }

    @Override
    public int getNumberOfTriples() {
      return headers.capacity() / Triple.HEADER_SIZE;
    }
    
  }


  

  

}
