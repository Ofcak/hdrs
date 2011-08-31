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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.ipc.Writable;

/**
 * File block representation used during file transfers.
 * 
 * @author daniel.hefenbrock
 *
 */
public class FileBlock implements Writable {
  ByteBuffer block;
  Triple firstTriple;
  
  public FileBlock() {
  }
  
  public FileBlock(ByteBuffer block, Triple firstTriple) {
    this.block = block;
    this.firstTriple = firstTriple;
  }
  
  public ByteBuffer getBlock() {
    return block;
  }

  public Triple getFirstTriple() {
    return firstTriple;
  }

  /**
   * @return the size of this FileBlock object (serialized),
   * NOT the size of the actual block.
   */
  public int size() {
    return block.limit() + firstTriple.serializedSize() 
        + Integer.SIZE/Byte.SIZE;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    firstTriple = Triple.readTriple(in);
    block = ByteBuffer.allocate(in.readInt());
    in.readFully(block.array(), 0, block.limit());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Triple.writeTriple(out, firstTriple);
    out.writeInt(block.limit());
    out.write(block.array(), block.arrayOffset(), block.limit());
  }
}
