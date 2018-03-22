package org.seqdoop.hadoop_bam.spark;

import com.google.common.io.Files;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.seqdoop.hadoop_bam.util.VCFHeaderReader;

@RunWith(JUnitParamsRunner.class)
public class VcfDatasetTest {

  private static JavaSparkContext jsc;

  @BeforeClass
  public static void setup() {
    jsc = new JavaSparkContext("local", "myapp");
  }

  @AfterClass
  public static void teardown() {
    jsc.stop();
  }

  private Object[] parametersForTestReadAndWrite() {
    return new Object[][] {
        {"file:///Users/tom/workspace/Hadoop-BAM/src/test/resources/test.vcf", ".vcf", 128 * 1024},
        {"file:///Users/tom/workspace/Hadoop-BAM/src/test/resources/test.vcf", ".vcf.gz", 128 * 1024},
        {"file:///Users/tom/workspace/Hadoop-BAM/src/test/resources/test.vcf", ".vcf.bgz", 128 * 1024},
    };
  }

  @Test
  @Parameters
  public void testReadAndWrite(String inputPath, String outputExtension, int splitSize) throws IOException {
    VcfDatasetFactory vcfDatasetFactory = VcfDatasetFactory.makeDefault(jsc)
        .splitSize(splitSize);

    JavaRDD<VariantContext> variants = vcfDatasetFactory
        .read(inputPath)
        .getVariantsRdd();

    int expectedCount = getVariantCount(new File(inputPath.replace("file://", "")));
    Assert.assertEquals(expectedCount, variants.count());

    File outputFile = File.createTempFile("test", outputExtension);
    outputFile.delete();
    String outputPath = outputFile.toURI().toString();

    FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();
    try (SeekableStream headerIn = fileSystemWrapper.open(jsc.hadoopConfiguration(), inputPath)) {
      vcfDatasetFactory
          .write(new VcfDataset(VCFHeaderReader.readHeaderFrom(headerIn), variants), outputPath);
    }
    if (outputExtension.endsWith(".gz") || outputExtension.endsWith(".bgz")) {
      Assert.assertTrue("block compressed", isBlockCompressed(outputFile));
    } else {
      Assert.assertFalse("block compressed", isBlockCompressed(outputFile));
    }
    Assert.assertEquals(expectedCount, getVariantCount(outputFile));
  }

  @Test
  public void testBgzfVcfIsSplitIntoMultiplePartitions() throws IOException {
    String inputPath = "file:///Users/tom/workspace/Hadoop-BAM/src/test/resources/HiSeq.10000.vcf.bgzf.gz";

    JavaRDD<VariantContext> variants = VcfDatasetFactory.makeDefault(jsc)
        .splitSize(128 * 1024)
        .read(inputPath)
        .getVariantsRdd();

    Assert.assertTrue(variants.getNumPartitions() > 1);

    int expectedCount = getVariantCount(new File(inputPath.replace("file://", "")));
    Assert.assertEquals(expectedCount, variants.count());
  }

  private static boolean isBlockCompressed(File file) throws IOException {
    try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
      return BlockCompressedInputStream.isValidFile(in);
    }
  }

  private static VCFFileReader parseVcf(File vcf) throws IOException {
    File actualVcf;
    // work around TribbleIndexedFeatureReader not reading header from .bgz files
    if (vcf.getName().endsWith(".bgz")) {
      actualVcf = File.createTempFile(vcf.getName(), ".gz");
      actualVcf.deleteOnExit();
      Files.copy(vcf, actualVcf);
    } else {
      actualVcf = vcf;
    }
    return new VCFFileReader(actualVcf, false);
  }

  private static int getVariantCount(final File vcf) throws IOException {
    final VCFFileReader vcfFileReader = parseVcf(vcf);
    final Iterator<VariantContext> it = vcfFileReader.iterator();
    int recCount = 0;
    while (it.hasNext()) {
      it.next();
      recCount++;
    }
    vcfFileReader.close();
    return recCount;
  }

}
