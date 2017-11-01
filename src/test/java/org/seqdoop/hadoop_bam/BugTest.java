package org.seqdoop.hadoop_bam;

import htsjdk.samtools.BAMFileReader;
import htsjdk.samtools.BAMFileSpan;
import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.Chunk;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.samtools.util.Interval;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.fail;

public class BugTest {

  @Test
  public void test1() {
    List<Interval> intervals = Arrays.asList(
        new Interval("chr15", 68820524, 68821028),
        new Interval("chr15", 78818650, 78818850),
        new Interval("chr15", 88072374, 88072957),
        new Interval("chr15", 94076779, 94076968),
        new Interval("chr16", 92489, 92801),
        new Interval("chr16", 474800, 475205),
        new Interval("chr16", 680065, 680747),
        new Interval("chr16", 4795630, 4796541),
        new Interval("chr16", 15631260, 15631630),
        new Interval("chr16", 20796026, 20799165),
        new Interval("chr16", 26736229, 26736317),
        new Interval("chr16", 30116502, 30117418),
        new Interval("chr16", 30929947, 30930708),
        new Interval("chr16", 31298950, 31299150),
        new Interval("chr16", 57174391, 57174936),
        new Interval("chr16", 58497862, 58498012),
        new Interval("chr16", 67146102, 67146445),
        new Interval("chr16", 67570985, 67571431),
        new Interval("chr16", 67868053, 67868705),
        new Interval("chr16", 67870670, 67872644),
        new Interval("chr16", 73486693, 73487010),
        new Interval("chr16", 74892346, 74892632),
        new Interval("chr16", 81889004, 81889408),
        new Interval("chr16", 82070814, 82072031),
        new Interval("chr17", 600830, 601127),
        new Interval("chr17", 16057406, 16058206),
        new Interval("chr17", 16058316, 16058755)
    );

    SamReaderFactory samReaderFactory = SamReaderFactory.makeDefault()
        .validationStringency(ValidationStringency.SILENT);
    File bam = new File("/home/tom/tmp/gatkspark_refname/gatkspark_refname.bam");
    SAMFileHeader header = samReaderFactory.getFileHeader(bam);

    QueryInterval[] queryIntervals = BAMInputFormat.prepareQueryIntervals(intervals, header.getSequenceDictionary());

    SamReader samReader = samReaderFactory.open(bam);
//    SAMRecordIterator query = samReader.query(queryIntervals, false);
//    SAMRecord next = query.next();
//    System.out.println(next);

    SamReader.PrimitiveSamReader primitiveSamReader =
        ((SamReader.PrimitiveSamReaderToSamReaderAdapter) samReader).underlyingReader();
    BAMFileReader bamFileReader = (BAMFileReader) primitiveSamReader;

    long[] fps = new long[] {635519759417674L, 635521956422799L};

//    CloseableIterator<SAMRecord> iterator = bamFileReader.createIndexIterator
//        (queryIntervals,
//        false, fps);
//    SAMRecord next2 = iterator.next();
//    System.out.println(next2);

    System.out.println(bamFileReader.getValidationStringency());

    BAMFileSpan splitSpan = new BAMFileSpan(new Chunk(fps[0], fps[1]));
    try {
      CloseableIterator<SAMRecord> iterator = bamFileReader.getIterator(splitSpan);
      SAMRecord next3 = iterator.next();
      System.out.println(next3);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testStream() throws IOException {
    File bam = new File("/home/tom/tmp/gatkspark_refname/gatkspark_refname.bam");

    SamReaderFactory samReaderFactory = SamReaderFactory.makeDefault()
        .validationStringency(ValidationStringency.SILENT);
    SAMFileHeader header = samReaderFactory.getFileHeader(bam);

    BAMRecordCodec bamCodec = new BAMRecordCodec(null, new LazyBAMRecordFactory());
    BlockCompressedInputStream bgzf = new BlockCompressedInputStream(bam);
    bgzf.seek(635519759417674L);
    bgzf.setCheckCrcs(true);

    bamCodec.setInputStream(bgzf);

    try {
      SAMRecord record = bamCodec.decode();
      record.setValidationStringency(ValidationStringency.SILENT);
      record.setHeader(header);
      record.isValid(false);
      record.getCigar(); // force decoding of CIGAR
      record.getCigarString();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

}
