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

package de.hpi.fgis.hdrs.node;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.FileImageOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.routing.SegmentInfo;
import de.hpi.fgis.hdrs.segment.Segment;
import de.hpi.fgis.hdrs.segment.SegmentConfiguration;
import de.hpi.fgis.hdrs.segment.SegmentIndex;

public class Index implements SegmentIndex {
  static final Log LOG = LogFactory.getLog(Index.class);
  
  /**
   * Directory where segments and index meta data are stored on disk.
   */
  private final File indexRoot;
  
  
  private final Triple.COLLATION order;
  
  
  private final SegmentIdGenerator idGenerator;
  
  
  /**
   * Ordered collection of segments.
   */
  private final SortedSet<SegmentInfo> segments;
  
  
  private final Map<Long, Integer> segmentRefCount;
    
  
  
  public static Index openIndex(File indexRoot, Triple.COLLATION order,
      SegmentIdGenerator idGen) throws IOException {
    if (!indexRoot.isDirectory()) {
      throw new IOException("Cannot create index: Index root directory does not exist");
    }
    Index index = new Index(indexRoot, order, idGen);
    index.readMeta();
    return index;
  }
  
  
  public static Index createIndex(File indexRoot, Triple.COLLATION order,
      SegmentIdGenerator idGen) throws IOException {
    if (!indexRoot.mkdir()) {
      throw new IOException("Couldn't create index root directory.");
    }
    Index index = new Index(indexRoot, order, idGen);
    index.writeMeta();
    return index;
  }
  
  
  /**
   * Create an index in the temp directory.
   * Used for testing.
   * @throws IOException 
   */
  public static Index createTmpIndex(Triple.COLLATION order) throws IOException {
    File root = File.createTempFile("hdrs_index_root", "");
    if (!root.delete()) {
      throw new IOException("Couldn't create tmp directory.");
    }
    Index index = createIndex(root, order, new SegmentIdGenerator() {
      private long id = 0;
      @Override
      public synchronized long generateSegmentId() {
        return ++id;
      }
    });
    // create initial segment
    index.createSeedSegment();
    return index;
  }
  
  
  private Index(File indexRoot, Triple.COLLATION order, SegmentIdGenerator gen) {
    this.indexRoot = indexRoot;
    this.order = order;
    this.idGenerator = gen;
    segments = new TreeSet<SegmentInfo>(SegmentInfo.getComparator(order));
    segmentRefCount = new HashMap<Long, Integer>();
  }
  
  
  public Triple.COLLATION getOrder() {
    return order;
  }
  
 
  // Testing
  public Segment openSegment(SegmentConfiguration conf, SegmentInfo info) 
  throws IOException {
    checkContains(info);
    return Segment.openSegment(conf, this, info.getSegmentId());
  }
  
  
  /**
   * Copies all segment infos into a list at returns it.
   * @return An ordered list of all segment infos currently in this index.
   */
  public List<SegmentInfo> getSegments() {
    synchronized (segments) {
      List<SegmentInfo> seglist = new ArrayList<SegmentInfo>(segments.size());
      for (SegmentInfo info : segments) {
        seglist.add(info);
      }
      return seglist;
    }
  }
  
  
  private void checkContains(SegmentInfo info) throws IOException {
    synchronized (segments) {
      if (!segments.contains(info)) {
        throw new IOException("segment not found in index");
      }
    }
  }
  
  
  public File getIndexRoot() {
    return indexRoot;
  }
  
  
  public File getSegmentDir(long segmentId) {
    return new File(indexRoot.getAbsolutePath() + File.separator 
        + "segment_" + segmentId);
  }
  
  
  /**
   * Create segment with range (*, *).
   * @throws IOException 
   */
  void createSeedSegment() throws IOException {
    createSegment(SegmentInfo.getSeed(idGenerator.generateSegmentId()));
  }
  
  void createSegment(SegmentInfo info) throws IOException {
    createSegment(info.getSegmentId());
    addSegment(info);
    writeMeta();
  }
  
  private long createSegment() throws IOException {
    long segmentId = idGenerator.generateSegmentId();
    createSegment(segmentId);
    return segmentId;
  }
  
  
  void createSegment(long segmentId) throws IOException {
    File dir = getSegmentDir(segmentId);
    int attempt = 0;
    while (!dir.mkdir()) {
      if (2 < attempt++) {
        throw new IOException("Couldn't create segment directory " + dir.getAbsolutePath());
      }
    }
    Segment.createSegment(this, segmentId);
  }
  
    
  private void addSegment(SegmentInfo info) throws IOException {
    synchronized (segments) {
      if (!segments.add(info)) {
        throw new IOException("error adding segment to index: " +
        		"segment does already exist");
      }
    }
  }
  
  
  /**
   * Delete this index and all of its segments.  For testing.
   * NOT THREAD SAFE.
   * @throws IOException 
   */
  public void delete() throws IOException {
    for (SegmentInfo info : segments) {
      Segment.deleteSegment(this, info.getSegmentId());
    }
    File metaFile = new File(indexRoot.getAbsolutePath() + File.separator + METAFILE);
    metaFile.delete();
    File indexRoot = getIndexRoot();
    if (!indexRoot.delete()) {
      throw new IOException("Couldn't delete index root directory.");
    }
  }
  
  
  public void setSegmentRefCount(long segmentId, int refCount) {
    synchronized (segmentRefCount) {
      segmentRefCount.put(segmentId, refCount);
    }
  }
  
  public void dereferenceSegment(long segmentId) throws IOException {
    int refCount;
    synchronized (segmentRefCount) {
      Integer intRc = segmentRefCount.get(Long.valueOf(segmentId));
      if (null == intRc) {
        LOG.warn("Segment reference count does not exist for segment " + segmentId 
            + ". Segment is likely to remain on disk as garbage.");
        return;
      }
      refCount = intRc.intValue();
      refCount--;
      if (0 == refCount) {
        segmentRefCount.remove(Long.valueOf(segmentId));
      } else {
        segmentRefCount.put(Long.valueOf(segmentId), Integer.valueOf(refCount));
      }
    }
    if (0 == refCount) {
      Segment.deleteSegment(this, segmentId);
    }
  }
  
  
  /**
   * 
   * Segment MUST be offline at this point.  Offlining a segment can be difficult
   * in the face of ongoing write transactions, flushes, and compactions.  Caller 
   * needs to take care of this.
   * 
   * Moreover, the segment MUST not contain half-files.
   * 
   * @param info
   * @param segment
   * @return
   * @throws IOException
   */
  public SegmentSplit splitSegment(SegmentInfo info, Segment segment) 
  throws IOException {
    if (info.getSegmentId() != segment.getId()) {
      throw new IOException("segment info / segment do not match");
    }
    checkContains(info);
    // generate segment ids
    long topId = createSegment();
    long bottomId = createSegment();
    // perform split and add child segments
    segment.takeOffline();
    Triple splitTriple = segment.split(topId, bottomId);
    if (null == splitTriple) {
      // problem, segment cannot be split
      return null;
    }
    // copy the split triple otherwise we hold on to the
    // block index buffer
    splitTriple = splitTriple.copy();
    segment.close();
    SegmentInfo top = new SegmentInfo(topId, info.getLowTriple(), splitTriple);
    SegmentInfo bottom = new SegmentInfo(bottomId, splitTriple, info.getHighTriple());
//    LOG.info(String.format("Segment %d split into segments %d and %d",
//        segment.getId(), topId, bottomId));
    SegmentSplit split = new SegmentSplit(top, bottom);
    return split;
  }
  
  
  /**
   * Do everything atomically and write meta data update.
   * @param info
   * @throws IOException 
   */
  synchronized void updateSegments(SegmentInfo remove, SegmentInfo add1, SegmentInfo add2) 
  throws IOException {
    // disable lookups
    synchronized (segments) {
      if (null != remove) {
        segments.remove(remove);
      }
      if (null != add1) {
        segments.add(add1);
      }
      if (null != add2) {
        segments.add(add2);
      }
    }
    writeMeta();
  }
  
  
  public static class SegmentNotFoundException extends Exception {
    private static final long serialVersionUID = 640874492093781997L;
    
    public SegmentNotFoundException() {
      super();
    }
    
    public SegmentNotFoundException(String msg) {
      super(msg);
    }
  }
  
  
  public static class SegmentSplit {
    public final SegmentInfo top;
    public final SegmentInfo bottom;
    
    public SegmentSplit(SegmentInfo top, SegmentInfo bottom) {
      this.top = top;
      this.bottom = bottom;
    }
  }
  
  
  private static final String METAFILE = "indexmeta";
  
  
  private void writeMeta() throws IOException {
    File metaFile = new File(indexRoot.getAbsolutePath() + File.separator + METAFILE);
    if (metaFile.isFile()) {
      if (!metaFile.delete()) {
        throw new IOException("Cannot delete old meta file for index " + order);
      }
    }
    FileImageOutputStream out = new FileImageOutputStream(metaFile);
    out.writeInt(segments.size());
    for (SegmentInfo info : segments) {
      info.write(out);
    }
    out.close();
  }
  
  void readMeta() throws IOException {
    File metaFile = new File(indexRoot.getAbsolutePath() + File.separator + METAFILE);
    if (!metaFile.isFile()) {
      throw new IOException("Index meta file not found for index " + order);
    }
    segments.clear();
    FileImageInputStream in = new FileImageInputStream(metaFile);
    int size = in.readInt();
    for (int i=0; i<size; ++i) {
      SegmentInfo info = SegmentInfo.read(in);
      segments.add(info);
    }
    in.close();
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}
