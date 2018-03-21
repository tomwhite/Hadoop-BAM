package org.seqdoop.hadoop_bam.spark;

import com.google.common.io.Files;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class VcfSourceTest {

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

    String path = "file:///Users/tom/workspace/Hadoop-BAM/src/test/resources/test.vcf";

    // find all the variants
    JavaRDD<VariantContext> variants = VcfDatasetFactory.makeDefault(jsc)
        .splitSize(splitSize)
        .read(path)
        .getVariantsRdd();

    int expectedCount = getVariantCount(new File(path.replace("file://", "")));
    Assert.assertEquals(expectedCount, variants.count());
  }

  @Test
  public void testBgzfVcf() throws IOException {
    int splitSize = 1 * 128 * 1024;

    String path = "file:///Users/tom/workspace/Hadoop-BAM/src/test/resources/HiSeq.10000.vcf.bgzf.gz";

    // find all the variants
    JavaRDD<VariantContext> variants = VcfDatasetFactory.makeDefault(jsc)
        .splitSize(splitSize)
        .read(path)
        .getVariantsRdd();

    Assert.assertTrue(variants.getNumPartitions() > 1);

    int expectedCount = getVariantCount(new File(path.replace("file://", "")));
    Assert.assertEquals(expectedCount, variants.count());
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

  static int getVariantCount(final File vcf) throws IOException {
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
