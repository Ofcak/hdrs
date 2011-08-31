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

package de.hpi.fgis.hdrs.mapreduce;

import org.junit.Assert;
import org.junit.Test;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.routing.SegmentInfo;

public class InputFormatTest {

  @Test
  public void testLogicalSplits() {
    
    TripleInputFormat format = new TripleInputFormat();
    
    Assert.assertArrayEquals(
        new IndexSplit[]{
            new IndexSplit(Triple.MAGIC_TRIPLE, Triple.MAGIC_TRIPLE)
            }, 
        format.getSplits(Triple.COLLATION.SPO, 3, new SegmentInfo[]{
            new SegmentInfo(0, Triple.MAGIC_TRIPLE, Triple.MAGIC_TRIPLE)
            }).toArray());
    
    
    Assert.assertArrayEquals(
        new IndexSplit[]{
            new IndexSplit(Triple.MAGIC_TRIPLE, Triple.newTriple("s", "p", "o")),
            new IndexSplit(Triple.newTriple("s", "p", "o"), Triple.MAGIC_TRIPLE)
            }, 
        format.getSplits(Triple.COLLATION.SPO, 3, new SegmentInfo[]{
            new SegmentInfo(0, Triple.MAGIC_TRIPLE, Triple.newTriple("s", "p", "o")),
            new SegmentInfo(0, Triple.newTriple("s", "p", "o"), Triple.MAGIC_TRIPLE)
            }).toArray());
    
    Assert.assertArrayEquals(
        new IndexSplit[]{
            new IndexSplit(Triple.MAGIC_TRIPLE, Triple.newTriple("a", "a", "a")),
            new IndexSplit(Triple.newTriple("a", "a", "a"), Triple.newTriple("b", "b", "b")),
            new IndexSplit(Triple.newTriple("b", "b", "b"), Triple.MAGIC_TRIPLE)
            }, 
        format.getSplits(Triple.COLLATION.SPO, 3, new SegmentInfo[]{
            new SegmentInfo(0, Triple.MAGIC_TRIPLE, Triple.newTriple("a", "a", "a")),
            new SegmentInfo(0, Triple.newTriple("a", "a", "a"), Triple.newTriple("b", "b", "b")),
            new SegmentInfo(0, Triple.newTriple("b", "b", "b"), Triple.MAGIC_TRIPLE)
            }).toArray());
    
    Assert.assertArrayEquals(
        new IndexSplit[]{
            new IndexSplit(Triple.MAGIC_TRIPLE, Triple.newPattern("b", "b", null)),
            new IndexSplit(Triple.newPattern("b", "b", null), Triple.MAGIC_TRIPLE)
            }, 
        format.getSplits(Triple.COLLATION.SPO, 2, new SegmentInfo[]{
            new SegmentInfo(0, Triple.MAGIC_TRIPLE, Triple.newTriple("a", "a", "a")),
            new SegmentInfo(0, Triple.newTriple("a", "a", "a"), Triple.newTriple("b", "b", "b")),
            new SegmentInfo(0, Triple.newTriple("b", "b", "b"), Triple.MAGIC_TRIPLE)
            }).toArray());
    
    Assert.assertArrayEquals(
        new IndexSplit[]{
            new IndexSplit(Triple.MAGIC_TRIPLE, Triple.newPattern("b", "b", null)),
            new IndexSplit(Triple.newPattern("b", "b", null), Triple.MAGIC_TRIPLE)
            }, 
        format.getSplits(Triple.COLLATION.SPO, 2, new SegmentInfo[]{
            new SegmentInfo(0, Triple.MAGIC_TRIPLE, Triple.newTriple("a", "a", "a")),
            new SegmentInfo(0, Triple.newTriple("a", "a", "a"), Triple.newTriple("a", "a", "b")),
            new SegmentInfo(0, Triple.newTriple("a", "a", "b"), Triple.newTriple("b", "b", "b")),
            new SegmentInfo(0, Triple.newTriple("b", "b", "b"), Triple.MAGIC_TRIPLE)
            }).toArray());
    
    Assert.assertArrayEquals(
        new IndexSplit[]{
            new IndexSplit(Triple.MAGIC_TRIPLE, Triple.newPattern("b", null, null)),
            new IndexSplit(Triple.newPattern("b", null, null), Triple.MAGIC_TRIPLE)
            }, 
        format.getSplits(Triple.COLLATION.SPO, 1, new SegmentInfo[]{
            new SegmentInfo(0, Triple.MAGIC_TRIPLE, Triple.newTriple("a", "a", "a")),
            new SegmentInfo(0, Triple.newTriple("a", "a", "a"), Triple.newTriple("a", "a", "b")),
            new SegmentInfo(0, Triple.newTriple("a", "a", "b"), Triple.newTriple("b", "b", "b")),
            new SegmentInfo(0, Triple.newTriple("b", "b", "b"), Triple.MAGIC_TRIPLE)
            }).toArray());
    
    Assert.assertArrayEquals(
        new IndexSplit[]{
            new IndexSplit(Triple.MAGIC_TRIPLE, Triple.newPattern("b", null, null)),
            new IndexSplit(Triple.newPattern("b", null, null), Triple.MAGIC_TRIPLE)
            }, 
        format.getSplits(Triple.COLLATION.SPO, 1, new SegmentInfo[]{
            new SegmentInfo(0, Triple.MAGIC_TRIPLE, Triple.newTriple("a", "a", "a")),
            new SegmentInfo(0, Triple.newTriple("a", "b", "a"), Triple.newTriple("a", "a", "b")),
            new SegmentInfo(0, Triple.newTriple("a", "a", "b"), Triple.newTriple("b", "b", "b")),
            new SegmentInfo(0, Triple.newTriple("b", "b", "b"), Triple.MAGIC_TRIPLE)
            }).toArray());
    
    Assert.assertArrayEquals(
        new IndexSplit[]{
            new IndexSplit(Triple.MAGIC_TRIPLE, Triple.newPattern("b", null, null)),
            new IndexSplit(Triple.newPattern("b", null, null), Triple.newPattern("c", null, null)),
            new IndexSplit(Triple.newPattern("c", null, null), Triple.MAGIC_TRIPLE)
            }, 
        format.getSplits(Triple.COLLATION.SPO, 1, new SegmentInfo[]{
            new SegmentInfo(0, Triple.MAGIC_TRIPLE, Triple.newTriple("a", "a", "a")),
            new SegmentInfo(0, Triple.newTriple("a", "a", "a"), Triple.newTriple("b", "b", "b")),
            new SegmentInfo(0, Triple.newTriple("b", "b", "b"), Triple.newTriple("c", "c", "c")),
            new SegmentInfo(0, Triple.newTriple("c", "c", "c"), Triple.MAGIC_TRIPLE)
            }).toArray());
    
    Assert.assertArrayEquals(
        new IndexSplit[]{
            new IndexSplit(Triple.newPattern("a", null, null), Triple.newPattern("a", "b", "c"))
            }, 
        format.getSplits(Triple.COLLATION.SPO, 1, new SegmentInfo[]{
            new SegmentInfo(0, Triple.newTriple("a", "b", "c"), Triple.newTriple("a", "b", "c"))
            }).toArray());
    
  }
  
}
