package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.cram.build.CramContainerIterator;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.io.CountingInputStream;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.ContainerIO;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IOUtils;

/**
 * Iterate over CRAM containers from an input stream, unlike {@link CramContainerIterator}
 * only the header of each container is read, rather than the whole stream. As a result, the
 * container data is *not* populated.
 */
public class CramContainerHeaderIterator implements Iterator<Container> {
  private CramHeader cramHeader;
  private SeekableStream inputStream;
  private Container nextContainer;
  private boolean eof = false;
  private long offset = 0;

  public CramContainerHeaderIterator(final SeekableStream inputStream) throws IOException {
    cramHeader = CramIO.readCramHeader(inputStream);
    offset = inputStream.position();
    this.inputStream = inputStream;
  }

  void readNextContainer() {
    try {
      long pos0 = inputStream.position();
      if (pos0 == 1478644) {
        final CountingInputStream cis = new CountingInputStream(inputStream);
        Container nc = ContainerIO.readContainer(cramHeader.getVersion(), cis);
        final long containerSizeInBytes = cis.getCount();
        //System.out.println("\t\ttw: nextContainer.containerByteSize: " + nc.containerByteSize);
        //System.out.println("\t\ttw: sequenceId: " + nc.sequenceId);
        //System.out.println("\t\ttw: alignmentStart: " + nc.alignmentStart);
        //System.out.println("\t\ttw: alignmentSpan: " + nc.alignmentSpan);
        //System.out.println("\t\ttw: nofRecords: " + nc.nofRecords);
        //System.out.println("\t\ttw: globalRecordCounter: " + nc.globalRecordCounter);
        //System.out.println("\t\ttw: bases: " + nc.bases);
        //System.out.println("\t\ttw: blockCount: " + nc.blockCount);
        //System.out.println("\t\ttw: num landmarks: " + nc.landmarks.length);
        inputStream.seek(pos0);
      }

      final CountingInputStream cis = new CountingInputStream(inputStream);
      nextContainer = ContainerIO.readContainerHeader(cramHeader.getVersion().major, cis);
      final long containerHeaderSizeInBytes = cis.getCount();
      nextContainer.offset = offset;

      long pos = inputStream.position();
      final long containerSizeInBytes = (pos - offset) + (long) nextContainer.containerByteSize; // containerByteSize excludes header
      //System.out.println("tw: offset: " + offset);
      //System.out.println("tw: pos0: " + pos0);
      //System.out.println("tw: pos: " + pos);
      //System.out.println("tw: containerHeaderSizeInBytes: " + containerHeaderSizeInBytes);
      //System.out.println("tw: nextContainer.containerByteSize: " + nextContainer.containerByteSize);
      //System.out.println("tw: containerSizeInBytes: " + containerSizeInBytes);
      //System.out.println("tw: offset + containerSizeInBytes: " + (offset + containerSizeInBytes));
      //System.out.println("\ttw: sequenceId: " + nextContainer.sequenceId);
      //System.out.println("\ttw: alignmentStart: " + nextContainer.alignmentStart);
      //System.out.println("\ttw: alignmentSpan: " + nextContainer.alignmentSpan);
      //System.out.println("\ttw: nofRecords: " + nextContainer.nofRecords);
      //System.out.println("\ttw: globalRecordCounter: " + nextContainer.globalRecordCounter);
      //System.out.println("\ttw: bases: " + nextContainer.bases);
      //System.out.println("\ttw: blockCount: " + nextContainer.blockCount);
      //System.out.println("\ttw: num landmarks: " + nextContainer.landmarks.length);
      //System.out.println();
      offset += containerSizeInBytes;
      //inputStream.seek(offset);
      IOUtils.skipFully(inputStream, nextContainer.containerByteSize);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    if (nextContainer.isEOF()) {
      eof = true;
      nextContainer = null;
    }
  }

  @Override
  public boolean hasNext() {
    if (eof) return false;
    if (nextContainer == null) readNextContainer();
    return !eof;
  }

  @Override
  public Container next() {
    final Container result = nextContainer;
    nextContainer = null;
    return result;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Read only iterator.");
  }

  public CramHeader getCramHeader() {
    return cramHeader;
  }

  public void close() {
    nextContainer = null;
    cramHeader = null;
    //noinspection EmptyCatchBlock
    try {
      inputStream.close();
    } catch (final Exception e) {
    }
  }
}
