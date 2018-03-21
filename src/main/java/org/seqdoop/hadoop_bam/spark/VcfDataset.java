package org.seqdoop.hadoop_bam.spark;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.spark.api.java.JavaRDD;

public class VcfDataset {

  private final VCFHeader header;
  private final JavaRDD<VariantContext> variants;

  public VcfDataset(VCFHeader header, JavaRDD<VariantContext> variants) {
    this.header = header;
    this.variants = variants;
  }

  public VCFHeader getFileHeader() {
    return header;
  }

  public JavaRDD<VariantContext> getVariantsRdd() {
    return variants;
  }
}
