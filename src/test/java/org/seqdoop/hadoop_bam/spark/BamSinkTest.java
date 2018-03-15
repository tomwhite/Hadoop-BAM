package org.seqdoop.hadoop_bam.spark;

import static org.seqdoop.hadoop_bam.spark.BamSourceTest.getBAMRecordCount;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.seekablestream.SeekableStream;
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
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;
import org.seqdoop.hadoop_bam.util.VCFHeaderReader;

public class BamSinkTest {

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

    File test = File.createTempFile("test", ".bam");
    test.delete();
    String outputPath = test.toURI().toString();

    FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();
    try (SeekableStream headerIn = fileSystemWrapper.open(jsc.hadoopConfiguration(), path)) {
      new BamSink().save(jsc, SAMHeaderReader.readSAMHeaderFrom(headerIn, jsc.hadoopConfiguration()), reads, outputPath);
    }

    int expectedCount = getBAMRecordCount(new File(path.replace("file://", "")));
    Assert.assertEquals(expectedCount, getBAMRecordCount(test));
  }

}
