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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

public class TripleSerializeTest {
 

  @Test
  public void testSlice() throws IOException {
  
    ByteBuffer buf =  ByteBuffer.allocate(1024);
  
    TripleSerializer serializer = new TripleSerializer(Triple.COLLATION.SPO, buf);
    
    serializer.add(Triple.newTriple("S1", "P1", "O1"));
    serializer.add(Triple.newTriple("S1", "P1", "O2"));
    serializer.add(Triple.newTriple("S1", "P2", "O3"));
    
    buf.flip();
    
    TripleDeserializer deserializer = new TripleDeserializer(Triple.COLLATION.SPO, buf);
    
    assertTrue(deserializer.next());
    deserializer.pop();
    assertTrue(deserializer.next());
    deserializer.pop();
    
    ByteBuffer nbuf = deserializer.slice();
    assertTrue(null != nbuf);
        
    deserializer = new TripleDeserializer(Triple.COLLATION.SPO, nbuf);
    
    assertTrue(deserializer.next());
    assertEquals(Triple.newTriple("S1", "P1", "O2"), deserializer.pop());
    assertTrue(deserializer.next());
    assertEquals(Triple.newTriple("S1", "P2", "O3"), deserializer.pop());
  }
  
  
  
  
}
