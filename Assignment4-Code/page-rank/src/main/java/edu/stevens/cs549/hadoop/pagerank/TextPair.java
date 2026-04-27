package edu.stevens.cs549.hadoop.pagerank;

// cc TextPair A Writable implementation that stores a pair of Text objects
// cc TextPairComparator A RawComparator for comparing TextPair byte representations
// cc TextPairFirstComparator A custom RawComparator for comparing the first field of TextPair byte representations
// vv TextPair

// Source: https://github.com/tomwhite/hadoop-book/blob/master/ch05-io/src/main/java/TextPair.java

import java.io.*;

import org.apache.hadoop.io.*;

/**
 * A WritableComparable that stores a pair of Text values.
 *
 * TextPair is used as the map output key in the join step.  The pair holds:
 *   first  — the join key (e.g. a vertex/node id) used to group records
 *   second — a sort tag ("0" for names, "1" for ranks) that controls the
 *            order in which the reducer sees values for the same join key
 *
 * Three comparator strategies are provided:
 *   - Natural order (compareTo / Comparator): orders by (first, second)
 *   - FirstComparator: orders and groups only by first, ignoring second —
 *     used as the Hadoop grouping comparator so all records for the same
 *     join key end up in the same reduce call regardless of their tag
 *   - KeyPartitioner (in PageRankDriver): partitions only on first so that
 *     all records for a join key go to the same reducer
 */
public class TextPair implements WritableComparable<TextPair> {

  private Text first;
  private Text second;

  /* ---- Constructors ---- */

  public TextPair() {
    set(new Text(), new Text());
  }
  
  public TextPair(String first, String second) {
    set(new Text(first), new Text(second));
  }
  
  public TextPair(Text first, Text second) {
    set(first, second);
  }

  /* ---- Accessors / mutator ---- */

  public void set(Text first, Text second) {
    this.first = first;
    this.second = second;
  }
  
  public Text getFirst() {
    return first;
  }

  public Text getSecond() {
    return second;
  }

  /* ---- Writable serialisation ---- */

  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);   // serialise first field
    second.write(out);  // serialise second field
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);   // deserialise first field
    second.readFields(in);  // deserialise second field
  }

  /* ---- Object identity ---- */

  @Override
  public int hashCode() {
    // Combine both field hashes using a prime multiplier to reduce collisions
    return first.hashCode() * 163 + second.hashCode();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof TextPair) {
      TextPair tp = (TextPair) o;
      return first.equals(tp.first) && second.equals(tp.second);
    }
    return false;
  }

  @Override
  public String toString() {
    return first + "\t" + second;
  }

  /* ---- Natural order: compare by (first, second) ---- */

  @Override
  public int compareTo(TextPair tp) {
    int cmp = first.compareTo(tp.first);
    if (cmp != 0) {
      return cmp;  // primary sort on first field
    }
    return second.compareTo(tp.second);  // tie-break on second field
  }

  // -----------------------------------------------------------------------
  // Byte-level comparator — avoids deserialising the full objects for speed
  // -----------------------------------------------------------------------

  public static class Comparator extends WritableComparator {
    
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
    
    public Comparator() {
      super(TextPair.class);
    }

    /**
     * Compare two serialised TextPair byte arrays in place.
     * Reads the variable-length encoding to find the boundary between
     * the first and second Text fields, then delegates to Text.Comparator.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      
      try {
        // Determine the byte length of the first field in each record
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);

        // Compare first fields
        int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
        if (cmp != 0) {
          return cmp;
        }
        // First fields are equal — compare second fields
        return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                                       b2, s2 + firstL2, l2 - firstL2);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  // Register Comparator as the default for TextPair
  static {
    WritableComparator.define(TextPair.class, new Comparator());
  }

  // -----------------------------------------------------------------------
  // FirstComparator — used as the Hadoop grouping comparator.
  // Groups records by first field only so all tags for the same join key
  // end up in one reduce() call.
  // -----------------------------------------------------------------------

  public static class FirstComparator extends WritableComparator {
    
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
    
    public FirstComparator() {
      super(TextPair.class);
    }

    /**
     * Compare only the first fields of two serialised TextPair byte arrays.
     * The second field (tag) is intentionally ignored for grouping purposes.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      
      try {
        // Determine the byte length of just the first field
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
        // Compare only the first fields; second field is ignored
        return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
    
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      if (a instanceof TextPair && b instanceof TextPair) {
        // Object-level variant: compare only the first fields
        return ((TextPair) a).first.compareTo(((TextPair) b).first);
      }
      return super.compare(a, b);
    }
  }
  
// vv TextPair
}
// ^^ TextPair}