package htsjdk.samtools;

public class SAMRecordHelper {
  public static void decodeEagerly(SAMRecord record) {
    record.eagerDecode();
  }
}
