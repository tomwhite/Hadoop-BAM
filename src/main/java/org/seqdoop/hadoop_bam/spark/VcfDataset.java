package org.seqdoop.hadoop_bam.spark;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.spark.api.java.JavaRDD;

/**
 * <p>
 * A {@link VcfDataset} is the distributed equivalent of a htsjdk {@link htsjdk.variant.vcf.VCFFileReader}.
 * It represents a VCF file stored in a distributed filesystem, and encapsulates a Spark RDD containing
 * the variant records in it.
 * </p>
 * <p>
 * Use a {@link VcfDatasetFactory} to read and write {@link VcfDataset}s.
 * </p>
 * @see VcfDatasetFactory
 */
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
