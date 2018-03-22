package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.ValidationStringency;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
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
  @Parameters({ "false", "true" })
  public void testFindAllReadStarts(boolean useNio) throws IOException, URISyntaxException {
    String inputPath = ClassLoader.getSystemClassLoader().getResource("1.bam").toURI().toString();
    int splitSize = 128 * 1024;

    // find all the read start positions in each partition
    JavaRDD<Long> readStarts = new BamSource(useNio)
        .getReadStarts(jsc, inputPath, splitSize, null, false, ValidationStringency.SILENT)
        .map(BamSource.ReadStart::getVirtualStart);

    Assert.assertEquals(Arrays.asList(45846L, 9065791718L, 17278959807L, 26929070350L, 34961096975L),
        readStarts.collect());
  }
}
