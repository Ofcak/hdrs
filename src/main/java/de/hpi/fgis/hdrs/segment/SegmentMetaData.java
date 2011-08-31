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

package de.hpi.fgis.hdrs.segment;

import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.FileImageOutputStream;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.node.Index.SegmentNotFoundException;
import de.hpi.fgis.hdrs.triplefile.HalfFileReader;
import de.hpi.fgis.hdrs.triplefile.TripleFileImporter;
import de.hpi.fgis.hdrs.triplefile.TripleFileReader;
import de.hpi.fgis.hdrs.triplefile.TripleFileWriter;
import de.hpi.fgis.hdrs.triplefile.HalfFileReader.HALF;

/**
 * <p> This class is responsible for persisting the meta data, i.e. triple
 * file information, of a segment.
 * 
 * <p> It also takes care creating and deleting triple files for segments.
 * 
 * @author daniel.hefenbrock
 *
 */
public class SegmentMetaData {

  private final SegmentIndex index;
  
  private final long segmentId;
  
  private final List<TripleFileReader> files;
  
  private Reference reference;
  
  
  private SegmentMetaData(SegmentIndex index, long segmentId, 
      List<TripleFileReader> files, Reference reference) {
    this.files = files;
    this.index = index;
    this.segmentId = segmentId;
    this.reference = reference;
  }
  
  
  public long getSegmentId() {
    return segmentId;
  }
  
  public Triple.COLLATION getOrder() {
    return index.getOrder();
  }
  
  public List<TripleFileReader> getFiles() {
    return new ArrayList<TripleFileReader>(files);
  }
  
  
  public SegmentIndex getIndex() {
    return index;
  }
  
  
  Triple split(long topId, long bottomId) throws IOException {
    // get split triple from largest file
    TripleFileReader splitFile = null;
    long size = 0;
    for (TripleFileReader file : files) {
      if (file.getFileSize() > size) {
        splitFile = file;
        size = file.getFileSize();
      }
    }
    Triple splitTriple = splitFile.getSplitTriple();
    // open splits
    SegmentMetaData top = load(index, topId);
    SegmentMetaData bottom = load(index, bottomId);
    // add half file references
    Reference topReference = new Reference();
    topReference.segmentId = segmentId;
    topReference.splitTriple = splitTriple;
    topReference.isTopHalf = true;
    top.reference = topReference;
    Reference bottomReference = new Reference();
    bottomReference.segmentId = segmentId;
    bottomReference.splitTriple = splitTriple;
    bottomReference.isTopHalf = false;
    bottom.reference = bottomReference;
    for (TripleFileReader file : files) {
      top.addFile(new HalfFileReader(file, splitTriple, HALF.TOP));
      bottom.addFile(new HalfFileReader(file, splitTriple, HALF.BOTTOM));
    }
    // reference parent segment
    index.setSegmentRefCount(getSegmentId(), 2*files.size());
    // write meta data 
    top.writeMetaDataFile(top.getMetaDataFile());
    bottom.writeMetaDataFile(bottom.getMetaDataFile());
    return splitTriple;
  }
  
  
  /**
   * Returns a handle to the file where the segment's meta data is stored in.
   * @return
   */
  private File getMetaDataFile() {
    return new File(getSegmentDirectory().getAbsoluteFile() + File.separator + METAFILE);
  }
  
  private File getSegmentDirectory() {
    return index.getSegmentDir(getSegmentId());
  }
  
  
  private File createFile() {
    File dir = getSegmentDirectory();
    return new File(dir.getAbsolutePath() + File.separator 
        + TRIPLEFILE + System.nanoTime());
  }
  
  
  /**
   * Used by segments to obtain a writer to a new triple files, e.g. when
   * flushing or compacting.
   * 
   * @return  Writer to a new triple file stored in the segment's directory.
   * @throws FileNotFoundException
   */
  TripleFileWriter createTripleFile() throws FileNotFoundException {
    File triplefile = createFile();
    // TODO read compression from conf
    return new TripleFileWriter(triplefile.getAbsolutePath(), 
        getOrder(), Compression.ALGORITHM.SNAPPY); 
  }
  
  
  TripleFileImporter createTripleFileImport(Compression.ALGORITHM compression,
      long dataSize, long nTriples) 
  throws FileNotFoundException {
    return new TripleFileImporter(createFile().getAbsolutePath(), getOrder(), compression,
        dataSize, nTriples);
  }
  
  
  /**
   * Used by segments to add a new file to the segment.
   * @param file  Reader for the new file.
   */
  void addFile(TripleFileReader file) {
    files.add(file);
  }
  
  
  /**
   * Remove indicated files from this segment.  This does not actually delete
   * the files from disk.
   * @param remove
   */
  void removeAllFiles(List<TripleFileReader> remove) {
    files.removeAll(remove);
  }
  
  
  
  /**
   * Delete all triple files, the meta data file, and the segment directory.
   * @throws IOException
   */
  void delete() throws IOException {
    for (TripleFileReader file : files) {
      deleteFile(file);
    }
    File meta = getMetaDataFile();
    if (!meta.delete()) {
      throw new IOException("Could not delete meta-data file.");
    }
    File dir = getSegmentDirectory();
    if (!dir.delete()) {
      throw new IOException("Could not delete segment directory.");
    }
  }
  
  
  void deleteFile(TripleFileReader file) throws IOException {
    if (file instanceof HalfFileReader) {
      index.dereferenceSegment(reference.segmentId);
    } else {
      file.delete();
    }
  }
  
  
  /**
   * NOT THREAD SAFE!!!
   * @return True, if no half file readers are present.
   */
  boolean canBeSplit() {
    for (TripleFileReader file : files) {
      if (file instanceof HalfFileReader) {
        return false;
      }
    }
    return true;
  }
  
  
  private void writeMetaDataFile(File file) throws IOException {
    FileImageOutputStream out = new FileImageOutputStream(file);
    write(out);
    out.close();
  }
  
  /**
   * Writes a new meta data file reflecting all changes made by the segment
   * (files added, removed).
   */
  void prepareWrite() throws IOException {
    File dir = getSegmentDirectory();
    File newMeta = new File(dir.getAbsoluteFile() + File.separator
        + METAFILE + ".pre");
    if (newMeta.isFile()) {
      throw new IOException("Could not prepare segment meta-data wrie.");
    }
    writeMetaDataFile(newMeta);
  }
  
  
  /**
   * Updates the actual meta data file by replacing it with the new
   * version produced by prepareWrite().
   */
  void commitWrite() throws IOException {
    File oldMeta = getMetaDataFile();
    File newMeta = new File(oldMeta.getAbsoluteFile() + ".pre");
    if (!newMeta.isFile()) {
      throw new IOException("Could not commit segment meta-data write.");
    }
    if (oldMeta.isFile()) {
      if (!oldMeta.delete()) {
        throw new IOException("Could not commit segment meta-data write.");
      }
    }
    if (!newMeta.renameTo(oldMeta)) {
      throw new IOException("Could not commit segment meta-data write.");
    }
  }
  
  
  private static final String METAFILE = "segmentmeta";
  private static final String TRIPLEFILE = "triplefile";
  
  private static final byte KEY_TRIPLE_FILE = 1;
  private static final byte KEY_HALF_FILE = 2;
  
  private void write(DataOutput out) throws IOException {
    out.writeLong(segmentId);
    // only write the reference if there are half files
    // this is checked by can be split.
    if (null != reference && !canBeSplit()) {
      out.writeByte(KEY_HALF_FILE);
      out.writeLong(reference.segmentId);
      Triple.writeTriple(out, reference.splitTriple);
      out.writeBoolean(reference.isTopHalf);
    } else {
      out.writeByte(KEY_TRIPLE_FILE);
    }
    // write file names separated by newlines
    for (TripleFileReader file : files) {
      if (file instanceof HalfFileReader) {
        out.writeByte(KEY_HALF_FILE);
      } else {
        out.writeByte(KEY_TRIPLE_FILE);
      }
      out.writeBytes(file.getFileName());
      out.writeByte('\n');
    }
  }
  
  
  /**
   * Called by segment.
   * @param index
   * @param segmentId
   * @return
   * @throws IOException
   * @throws SegmentNotFoundException 
   */
  static SegmentMetaData load(SegmentIndex index, long segmentId) throws IOException {
    File dir = index.getSegmentDir(segmentId);
    if (!dir.isDirectory()) {
      throw new IOException("Segment directory not found for segment " + segmentId);
    }
    File metaFile = new File(dir.getAbsolutePath() + File.separator + METAFILE);
    if (!metaFile.isFile()) {
      throw new IOException("Segment meta-data not found for segment " + segmentId);
    }
    FileImageInputStream input = new FileImageInputStream(metaFile);
    // sanity check
    if (segmentId != input.readLong()) {
      throw new IOException("Wrong segment id in segment info file.");
    }
    
    Reference reference = null;
    File referencedSegmentDir = null;
    
    if (KEY_HALF_FILE == input.readByte()) {
      // this segment contains half files.
      reference = new Reference();
      reference.segmentId = input.readLong();
      referencedSegmentDir = index.getSegmentDir(reference.segmentId);
      if (!referencedSegmentDir.isDirectory()) {
        throw new IOException("Error while reading segment meta-data of segment "
            + segmentId + ".  Segment references triple file of segment " 
            + reference.segmentId + " whose segment directory does not exist.");
      }
      reference.splitTriple = Triple.readTriple(input);
      reference.isTopHalf = input.readBoolean();
    }
    
    List<TripleFileReader> files = new ArrayList<TripleFileReader>();
   
    while (true) {
      boolean isHalfFile;
      try {
        isHalfFile = KEY_HALF_FILE == input.readByte();
      } catch (EOFException eof) {
        break;
      }
      String filename = input.readLine();
      if (null == filename) {
        throw new IOException("Error while reading segment meta-data.");
      }
      if (isHalfFile) {
        File halffile = new File(referencedSegmentDir.getAbsolutePath() 
            + File.separator + filename);
        if (!halffile.isFile()) {
          throw new IOException("Triple file not found while opening segment: " + halffile);
        }
        files.add(new HalfFileReader(halffile, reference.splitTriple, 
            reference.isTopHalf ? HALF.TOP : HALF.BOTTOM));
      } else {
        File triplefile = new File(dir.getAbsolutePath() + File.separator + filename);
        if (!triplefile.isFile()) {
          //throw new IOException("Triple file not found while opening segment: " + triplefile);
          continue;
        }
        files.add(new TripleFileReader(triplefile));
      }
    }
    input.close();
    return new SegmentMetaData(index, segmentId, files, reference);
  }
  
  
  /**
   * Called by segment.
   * @param index
   * @param segmentId
   * @return
   * @throws IOException
   * @throws SegmentNotFoundException 
   */
  static SegmentMetaData create(SegmentIndex index, long segmentId) 
  throws IOException {
    SegmentMetaData meta = new SegmentMetaData(index, segmentId, 
        new ArrayList<TripleFileReader>(), null); 
    // write initial segment info file
    meta.writeMetaDataFile(meta.getMetaDataFile());
    return meta;
  }
  
  
  public static class MetaDataPair {
    public final SegmentMetaData first;
    public final SegmentMetaData second;
    public MetaDataPair(SegmentMetaData first, SegmentMetaData second) {
      this.first = first;
      this.second = second;
    }
  }
  
  private static class Reference {
    long segmentId;
    Triple splitTriple;
    boolean isTopHalf;
  }
  
}
