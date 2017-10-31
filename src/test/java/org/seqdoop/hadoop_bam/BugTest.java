package org.seqdoop.hadoop_bam;

import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.util.Interval;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class BugTest {

  @Test
  public void test1() {
    List<Interval> intervals = Arrays.asList(
        new Interval("14", 68820524, 68821028),
        new Interval("14", 78818650, 78818850),
        new Interval("14", 88072374, 88072957),
        new Interval("14", 94076779, 94076968),
        new Interval("15", 92489, 92801),
        new Interval("15", 474800, 475205),
        new Interval("15", 680065, 680747),
        new Interval("15", 4795630, 4796541),
        new Interval("15", 15631260, 15631630),
        new Interval("15", 20796026, 20799165),
        new Interval("15", 26736229, 26736317),
        new Interval("15", 30116502, 30117418),
        new Interval("15", 30929947, 30930708),
        new Interval("15", 31298950, 31299150),
        new Interval("15", 57174391, 57174936),
        new Interval("15", 58497862, 58498012),
        new Interval("15", 67146102, 67146445),
        new Interval("15", 67570985, 67571431),
        new Interval("15", 67868053, 67868705),
        new Interval("15", 67870670, 67872644),
        new Interval("15", 73486693, 73487010),
        new Interval("15", 74892346, 74892632),
        new Interval("15", 81889004, 81889408),
        new Interval("15", 82070814, 82072031),
        new Interval("16", 600830, 601127),
        new Interval("16", 16057406, 16058206),
        new Interval("16", 16058316, 16058755)
    );

    SamReaderFactory samReaderFactory = SamReaderFactory.makeDefault();
    File bam = new File("/home/tom/tmp/gatkspark_refname/gatkspark_refname.bam");
    SAMFileHeader header = samReaderFactory.getFileHeader(bam);

    QueryInterval[] queryIntervals = BAMInputFormat.prepareQueryIntervals(intervals, header.getSequenceDictionary());

    SamReader samReader = samReaderFactory.open(bam);
    SAMRecordIterator query = samReader.query(queryIntervals, false);
    SAMRecord next = query.next();
    System.out.println(next);
  }

}
