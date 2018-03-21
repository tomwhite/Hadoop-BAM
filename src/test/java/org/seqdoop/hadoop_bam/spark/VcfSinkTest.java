package org.seqdoop.hadoop_bam.spark;

import static org.seqdoop.hadoop_bam.spark.VcfSourceTest.getVariantCount;

import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.variant.variantcontext.VariantContext;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seqdoop.hadoop_bam.util.VCFHeaderReader;

public class VcfSinkTest {

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
  public void testUncompressed() throws IOException {
    int splitSize = 1 * 128 * 1024;

    VcfDatasetFactory vcfDatasetFactory = VcfDatasetFactory.makeDefault(jsc)
        .splitSize(splitSize);

    String path = "file:///Users/tom/workspace/Hadoop-BAM/src/test/resources/test.vcf";

    // find all the variants
    JavaRDD<VariantContext> variants = vcfDatasetFactory
        .read(path)
        .getVariantsRdd();

    File test = File.createTempFile("test", ".vcf");
    test.delete();
    String outputPath = test.toURI().toString();

    FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();
    try (SeekableStream headerIn = fileSystemWrapper.open(jsc.hadoopConfiguration(), path)) {
      vcfDatasetFactory
          .write(new VcfDataset(VCFHeaderReader.readHeaderFrom(headerIn), variants), outputPath);
    }
    Assert.assertFalse("block compressed", isBlockCompressed(test));
    int expectedCount = getVariantCount(new File(path.replace("file://", "")));
    Assert.assertEquals(expectedCount, getVariantCount(test));
  }

  @Test
  public void testCompressed() throws IOException {
    int splitSize = 1 * 128 * 1024;

    VcfDatasetFactory vcfDatasetFactory = VcfDatasetFactory.makeDefault(jsc)
        .splitSize(splitSize);

    String path = "file:///Users/tom/workspace/Hadoop-BAM/src/test/resources/test.vcf";

    // find all the variants
    JavaRDD<VariantContext> variants = vcfDatasetFactory
        .read(path)
        .getVariantsRdd();

    File test = File.createTempFile("test", ".vcf.gz");
    test.delete();
    String outputPath = test.toURI().toString();

    FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();
    try (SeekableStream headerIn = fileSystemWrapper.open(jsc.hadoopConfiguration(), path)) {
      vcfDatasetFactory
          .write(new VcfDataset(VCFHeaderReader.readHeaderFrom(headerIn), variants), outputPath);
    }
    Assert.assertTrue("block compressed", isBlockCompressed(test));
    int expectedCount = getVariantCount(new File(path.replace("file://", "")));
    Assert.assertEquals(expectedCount, getVariantCount(test));
  }

  private static boolean isBlockCompressed(File file) throws IOException {
    try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
      return BlockCompressedInputStream.isValidFile(in);
    }
  }

}
