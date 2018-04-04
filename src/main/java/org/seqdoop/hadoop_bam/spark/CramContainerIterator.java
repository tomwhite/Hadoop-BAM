package org.seqdoop.hadoop_bam.spark;


import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.io.CountingInputStream;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.ContainerIO;
import htsjdk.samtools.cram.structure.CramHeader;

import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * An iterator of CRAM containers read from an {@link java.io.InputStream}.
 */
public class CramContainerIterator implements Iterator<Container> {
  private CramHeader cramHeader;
  private SeekableStream inputStream;
  private Container nextContainer;
  private boolean eof = false;
  private long offset = 0;
  private long count;

  public CramContainerIterator(final SeekableStream inputStream) throws IOException {
    cramHeader = CramIO.readCramHeader(inputStream);
    this.inputStream = inputStream;
  }

  void readNextContainer() {
    try {
      //System.out.println("tw: pos: " + inputStream.position());

      final CountingInputStream cis = new CountingInputStream(inputStream);
      nextContainer = ContainerIO.readContainer(cramHeader.getVersion(), cis);
      final long containerSizeInBytes = cis.getCount();

      nextContainer.offset = offset;

      //System.out.println("tw: offset: " + offset);
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
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    if (nextContainer.isEOF()) {
      eof = true;
      nextContainer = null;
    }

    if (count++ % 1000 == 0) {
      System.out.println("Containers processed: " + count);
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
