package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BamSourceTest {

  private static JavaSparkContext jsc;

  @BeforeClass
  public static void setup() {
    jsc = new JavaSparkContext("local", "myapp");
  }

  @AfterClass
  public static void teardown() {
    jsc.stop();
  }

  @Test
  public void test() throws IOException {
    int splitSize = 1 * 128 * 1024;

    //String path = "file:///Users/tom/tmp/HG00103.unmapped.ILLUMINA.bwa.GBR.low_coverage.20120522.bam";
    String path = "file:///Users/tom/workspace/spark-bam/test_bams/src/main/resources/1.bam";

    // find all the reads
    JavaRDD<SAMRecord> reads = new BamSource().getReads(jsc, path, splitSize);

    int expectedCount = getBAMRecordCount(new File(path.replace("file://", "")));
    Assert.assertEquals(expectedCount, reads.count());
  }

  @Test
  public void testMultiple() throws IOException {
    int splitSize = 1 * 128 * 1024;

    // directory containing two BAM files
    String path = "file:///Users/tom/workspace/gatk//src/test/resources/org/broadinstitute/hellbender/tools/BQSR/HiSeq.1mb.1RG.2k_lines.alternate.recalibrated.DIQ.sharded.bam";

    // find all the reads
    JavaRDD<SAMRecord> reads = new BamSource().getReads(jsc, path, splitSize);

    //int expectedCount = getBAMRecordCount(new File(path.replace("file://", "")));
    int expectedCount = getBAMRecordCount(new File("/Users/tom/workspace/gatk//src/test/resources/org/broadinstitute/hellbender/tools/BQSR/HiSeq.1mb.1RG.2k_lines.alternate.recalibrated.DIQ.sharded.bam/part-r-00000.bam")) +
        getBAMRecordCount(new File("/Users/tom/workspace/gatk//src/test/resources/org/broadinstitute/hellbender/tools/BQSR/HiSeq.1mb.1RG.2k_lines.alternate.recalibrated.DIQ.sharded.bam/part-r-00001.bam"));
    Assert.assertEquals(expectedCount, reads.count());
  }

  @Test
  public void testFindAllReadStarts() throws IOException {
    int splitSize = 1 * 128 * 1024;

    String path = "file:///Users/tom/workspace/spark-bam/test_bams/src/main/resources/1.bam";

    // find all the read start positions in each partition
    JavaRDD<Long> readStarts = new BamSource().getReadStarts(jsc, path, splitSize, null, false).map(BamSource.ReadStart::getVirtualStart);

    Assert.assertEquals(Arrays.asList(45846L, 9065791718L, 17278959807L, 26929070350L, 34961096975L),
        readStarts.collect());

  }

  @Test
  public void testNio() throws IOException {
    int splitSize = 1 * 128 * 1024;

    String path = "file:///Users/tom/workspace/spark-bam/test_bams/src/main/resources/1.bam";

    // find all the reads
    JavaRDD<SAMRecord> reads = new BamSource(false).getReads(jsc, path, splitSize);

    int expectedCount = getBAMRecordCount(new File(path.replace("file://", "")));
    Assert.assertEquals(expectedCount, reads.count());
  }

  @Test
  public void testFindAllReadStartsNio() throws IOException {
    int splitSize = 1 * 128 * 1024;

    String path = "file:///Users/tom/workspace/spark-bam/test_bams/src/main/resources/1.bam";

    // find all the read start positions in each partition
    JavaRDD<Long> readStarts = new BamSource(false).getReadStarts(jsc, path, splitSize, null, false).map(BamSource.ReadStart::getVirtualStart);

    Assert.assertEquals(Arrays.asList(45846L, 9065791718L, 17278959807L, 26929070350L, 34961096975L),
        readStarts.collect());

  }

  static int getBAMRecordCount(final File bamFile) throws IOException {
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
