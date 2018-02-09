package org.seqdoop.hadoop_bam.spark.cli;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;
import org.seqdoop.hadoop_bam.spark.cli.CountReads;

public class CountReadsTest {
  @Test
  public void testCountReads() throws IOException {
    int splitSize = 1 * 128 * 1024;

    String path = "file:///Users/tom/workspace/spark-bam/test_bams/src/main/resources/1.bam";

    int expectedCount = getBAMRecordCount(new File(path.replace("file://", "")));
    Assert.assertEquals(expectedCount, CountReads.countReads(path, "local", splitSize));
  }

  @Test
  public void testCountReadsNio() throws IOException {
    int splitSize = 1 * 128 * 1024;

    String path = "file:///Users/tom/workspace/spark-bam/test_bams/src/main/resources/1.bam";

    int expectedCount = getBAMRecordCount(new File(path.replace("file://", "")));
    Assert.assertEquals(expectedCount, CountReads.countReadsNio(path, "local", splitSize));
  }

  @Test
  public void testCountReadsLegacy() throws IOException {
    int splitSize = 1 * 128 * 1024;

    String path = "file:///Users/tom/workspace/spark-bam/test_bams/src/main/resources/1.bam";

    int expectedCount = getBAMRecordCount(new File(path.replace("file://", "")));
    Assert.assertEquals(expectedCount, CountReads.countReadsLegacy(path, "local", splitSize));
  }

  private static int getBAMRecordCount(final File bamFile) throws IOException {
    final SamReader bamReader = SamReaderFactory.makeDefault()
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
