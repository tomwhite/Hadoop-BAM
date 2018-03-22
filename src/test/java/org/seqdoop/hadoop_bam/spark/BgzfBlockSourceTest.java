package org.seqdoop.hadoop_bam.spark;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.seqdoop.hadoop_bam.spark.BgzfBlockGuesser.BgzfBlock;

@RunWith(JUnitParamsRunner.class)
public class BgzfBlockSourceTest extends BaseTest {
  
  @Test
  @Parameters({ "false", "true" })
  public void testFindAllBlocks(boolean useNio) throws IOException, URISyntaxException {
    String inputPath = ClassLoader.getSystemClassLoader().getResource("1.bam").toURI().toString();
    int splitSize = 128 * 1024;

    // find all the blocks in each partition
    JavaRDD<BgzfBlock> bgzfBlocks = new BgzfBlockSource(useNio).getBgzfBlocks(jsc, inputPath, splitSize);
    List<BgzfBlock> collect = bgzfBlocks.collect();

    Assert.assertEquals(26, collect.size());

    Assert.assertEquals(0, collect.get(0).pos);
    Assert.assertEquals(14146, collect.get(0).cSize);
    Assert.assertEquals(65498, collect.get(0).uSize);
  }
}
