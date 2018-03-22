package org.seqdoop.hadoop_bam.spark.cli;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.cram.ref.ReferenceSource;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;

public class CountReadsTest {
  @Test
  public void testCountReads() throws IOException {
    String path = "file:///Users/tom/workspace/spark-bam/test_bams/src/main/resources/1.bam";
    int splitSize = 128 * 1024;

    int expectedCount = getBAMRecordCount(new File(path.replace("file://", "")));
    Assert.assertEquals(expectedCount, CountReads.countReads(path, "local", splitSize, null));
  }

  @Test
  public void testCountReadsCram() throws IOException {
    String path = "file:///Users/tom/workspace/Hadoop-BAM/src/test/resources/test.cram";
    String referenceSource = "file:///Users/tom/workspace/Hadoop-BAM/src/test/resources/auxf.fa";
    int splitSize = 128 * 1024;

    int expectedCount = getBAMRecordCount(new File(path.replace("file://", "")),
        new ReferenceSource(new File(URI.create(referenceSource))));
    Assert.assertEquals(expectedCount, CountReads.countReads(path, "local", splitSize, referenceSource));
  }

  @Test
  public void testCountReadsNio() throws IOException {
    String path = "file:///Users/tom/workspace/spark-bam/test_bams/src/main/resources/1.bam";
    int splitSize = 128 * 1024;

    int expectedCount = getBAMRecordCount(new File(path.replace("file://", "")));
    Assert.assertEquals(expectedCount, CountReads.countReadsNio(path, "local", splitSize));
  }

  @Test
  public void testCountReadsLegacy() throws IOException {
    String path = "file:///Users/tom/workspace/spark-bam/test_bams/src/main/resources/1.bam";
    int splitSize = 128 * 1024;

    int expectedCount = getBAMRecordCount(new File(path.replace("file://", "")));
    Assert.assertEquals(expectedCount, CountReads.countReadsLegacy(path, "local", splitSize));
  }

  private static int getBAMRecordCount(final File bamFile) throws IOException {
    return getBAMRecordCount(bamFile, null);
  }

  private static int getBAMRecordCount(final File bamFile, ReferenceSource referenceSource) throws IOException {
    final SamReader bamReader = SamReaderFactory.makeDefault()
        .referenceSource(referenceSource)
        .open(SamInputResource.of(bamFile));
    final Iterator<SAMRecord> it = bamReader.iterator();
    int recCount = 0;
    while (it.hasNext()) {
      it.next();
      recCount++;
    }
    bamReader.close();
    return recCount;
  }
}
