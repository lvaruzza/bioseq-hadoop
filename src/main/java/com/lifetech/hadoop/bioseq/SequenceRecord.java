package com.lifetech.hadoop.bioseq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class SequenceRecord implements WritableComparable<SequenceRecord> {

	public static class Comparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public Comparator() {
			super(SequenceRecord.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2,
						firstL2);
				if (cmp != 0) {
					return cmp;
				}
				return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
						b2, s2 + firstL2, l2 - firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	  public static class HeaderComparator extends WritableComparator {
		    
		    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		    
		    public HeaderComparator() {
		      super(SequenceRecord.class);
		    }

		    @Override
		    public int compare(byte[] b1, int s1, int l1,
		                       byte[] b2, int s2, int l2) {
		      
		      try {
		        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
		        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
		        return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
		      } catch (IOException e) {
		        throw new IllegalArgumentException(e);
		      }
		    }
		    
		    @Override
		    public int compare(WritableComparable a, WritableComparable b) {
		      if (a instanceof SequenceRecord && b instanceof SequenceRecord) {
		        return ((SequenceRecord) a).header.compareTo(((SequenceRecord) b).header);
		      }
		      return super.compare(a, b);
		    }
		  }
	  
	static {
		//WritableComparator.define(SequenceRecord.class, new Comparator());
		WritableComparator.define(SequenceRecord.class, new HeaderComparator());
	}

	private Text header;
	private Text sequence;

	public SequenceRecord() {
		set(new Text(), new Text());
	}

	public SequenceRecord(String header, String sequence) {
		set(new Text(header), new Text(sequence));
	}

	public SequenceRecord(Text first, Text second) {
		set(first, second);
	}

	public void set(Text header, Text sequence) {
		this.header = header;
		this.sequence = sequence;
	}

	public Text getHeader() {
		return header;
	}

	public Text getSequence() {
		return sequence;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		header.write(out);
		sequence.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		header.readFields(in);
		sequence.readFields(in);
	}

	@Override
	public int hashCode() {
		return header.hashCode() * 163 + sequence.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof SequenceRecord) {
			SequenceRecord tp = (SequenceRecord) o;
			return header.equals(tp.header) && sequence.equals(tp.sequence);
		}
		return false;
	}

	@Override
	public String toString() {
		return header + "\t" + sequence;
	}

	@Override
	public int compareTo(SequenceRecord tp) {
		int cmp = header.compareTo(tp.header);
		if (cmp != 0) {
			return cmp;
		}
		return sequence.compareTo(tp.sequence);
	}
}
