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

package de.hpi.fgis.hdrs.triplefile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.TripleDeserializer;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.compression.BlockDecompressor;
import de.hpi.fgis.hdrs.compression.Compression.ALGORITHM;
import de.hpi.fgis.hdrs.tio.TripleScanner;

/**
 * The triple file importer is used during segment transfers.  Triple files are
 * transfered block-by-block during a segment transfer.  This class manages
 * writing the blocks to disk, adding the block index and trailer when done.
 * 
 * @author daniel.hefenbrock
 *
 */
public class TripleFileImporter extends TripleFileWriter implements FileImport {

  private final BlockDecompressor decompressor;
  private FileBlock lastBlock = null;
  
  private TransferThread thread = null;
  private FileBlock[] blocks = null;
  private final Semaphore producer = new Semaphore(1);
  private final Semaphore consumer = new Semaphore(0);
  
  // for testing
  public TripleFileImporter(String filename, COLLATION order,
      ALGORITHM compression) throws FileNotFoundException {
    this(filename, order, compression, -1, -1);
  }
  
  public TripleFileImporter(String filename, COLLATION order,
      ALGORITHM compression, long dataSize, long nTriples) throws FileNotFoundException {
    super(filename, order, compression);
    decompressor = compress() ? compression.getDecompressor() : null;
    this.dataSize = dataSize;
    this.tripleCount = (int) (0 == nTriples ? -1 : nTriples); // never pass 0 here
  }
  
  /*public TripleFileImporter(File file, COLLATION order,
      ALGORITHM compression) throws FileNotFoundException {
    super(file, order, compression);
  }*/
  
  
  private class TransferThread extends Thread {

    private volatile boolean done = false;
    private volatile IOException exception = null;
    
    @Override
    public void run() {
      
      while (true) {
        consumer.acquireUninterruptibly();
        if (done) {
          return;
        }
        try {
          for (int i=0; i<blocks.length; ++i) {
            FileBlock block = blocks[i];
            indexedTriples.add(block.getFirstTriple());
            indexOffsets.add(Long.valueOf(fileOffset()));
//            indexSize += Long.SIZE / Byte.SIZE + block.getFirstTriple().serializedSize();

            uncompressedBytes += compress() 
                ? decompressor.uncompressedLength(
                    block.getBlock().array(), 0, block.getBlock().limit())
                : block.getBlock().limit();
      
            if (compress()) {
              compressedBytes += block.getBlock().limit();
            }
      
            out.write(block.getBlock().array(), block.getBlock().arrayOffset(), 
                block.getBlock().limit());  
            lastBlock = block;
          }
        } catch (IOException ioe) {
          exception = ioe;
          return;
        } finally {
          producer.release();
        }
      }
    }
    
    IOException getException() {
      return exception;
    }
    
    void quit() throws IOException {
      producer.acquireUninterruptibly();
      done = true;
      consumer.release();
      try {
        join();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      if (null != exception) {
        throw exception;
      }
    }
  }
  
  
  public void addBlocks(FileBlock[] blocks) throws IOException {
    
    producer.acquireUninterruptibly();
    
    if (null == thread) {
      thread = new TransferThread();
      thread.start();
    } else if (null != thread.getException()) {
      throw thread.getException();
    }
    
    this.blocks = blocks;
    
    consumer.release();
  }
  
  public boolean close(Triple lastTriple) throws IOException {
    if (null != thread) {
      thread.quit();
    }
    if (null == lastBlock) {
      // there wasn't even a single block?!  delete file.
      // since tripleCount == 0, this will delete it.
      tripleCount = 0;
      super.close();
      return false;
    }
    if (null == lastTriple) {
      // this sucks ass, but there's no way around it.
      // we need to inflate and read the entire last block
      // in order to get the last triple of this file.
      ByteBuffer buffer = lastBlock.getBlock();
      if (compress()) {
        ByteBuffer uncompressed = ByteBuffer.allocate(
            decompressor.uncompressedLength(buffer.array(), 0, buffer.limit()));
        decompressor.uncompress(buffer.array(), 0, buffer.limit(), uncompressed.array(), 0);
        buffer = uncompressed;
      }
      // scan to last triple...
      TripleScanner scanner = new TripleDeserializer(getOrder(), buffer);
      while (scanner.next()) {
        lastTriple = scanner.pop();
      }
      scanner.close();
    }
    this.lastBlock = null;
    this.lastTriple = lastTriple.copy();
    super.close();
    this.lastTriple = null;
    return true;
  }
  
  @Override
  public void close() throws IOException {
    close(null);
  }
  
  @Override
  protected void finishBlock() {
    // NOOP
  }

}
