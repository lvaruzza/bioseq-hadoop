package com.lifetech.hadoop.bioseq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/*
 * Writable Bio Seequence with id, sequence and quality
 * 
 */
public class BioSeqWritable implements Writable,WritableComparable<BioSeqWritable> {

	public static class Comparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public Comparator() {
			super(BioSeqWritable.class);
		}

		/* TOFINISH
		 * (non-Javadoc)
		 * @see org.apache.hadoop.io.WritableComparator#compare(byte[], int, int, byte[], int, int)
		 */
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2,firstL2);
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

	/*static {
		WritableComparator.define(WritableBioSeq.class, new Comparator());
	}*/

	public Text id;
	public Text sequence;
	public Text quality;

	public BioSeqWritable(Text id, Text sequence, Text quality) {
		this.id = id;
		this.quality = quality;
		this.sequence = sequence;
	}

	/*
	 *  Mostly For debug use
	 */
	public BioSeqWritable(String id, String sequence, String quality) {
		this.id = id == null ? null : new Text(id);
		this.quality = quality == null ? null : new Text(quality);
		this.sequence = sequence == null ? null : new Text(sequence);
	}

	public BioSeqWritable() {
		id = null;
		quality = null;
		sequence = null;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		id.readFields(input);
		sequence.readFields(input);
		quality.readFields(input);

	}

	@Override
	public void write(DataOutput output) throws IOException {
		id.write(output);
		sequence.write(output);
		quality.write(output);
	}

	public Text getId() {
		return id;
	}

	public Text getSequence() {
		return sequence;
	}

	public Text getQuality() {
		return quality;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((quality == null) ? 0 : quality.hashCode());
		result = prime * result
				+ ((sequence == null) ? 0 : sequence.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BioSeqWritable other = (BioSeqWritable) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (quality == null) {
			if (other.quality != null)
				return false;
		} else if (!quality.equals(other.quality))
			return false;
		if (sequence == null) {
			if (other.sequence != null)
				return false;
		} else if (!sequence.equals(other.sequence))
			return false;
		return true;
	}

	@Override
	public int compareTo(BioSeqWritable otherSeq) {
		int cmp = id.compareTo(otherSeq.id);
		if (cmp != 0) {
			return cmp;
		}
		cmp = sequence.compareTo(otherSeq.sequence);
		if (cmp != 0) {
			return cmp;
		}
		return quality.compareTo(otherSeq.quality);
	}

	public void set(Text id, Text sequence, Text quality) {
		this.id = id;
		this.quality = quality;
		this.sequence = sequence;
	}

	@Override
	public String toString() {
		if (quality == null) {
			return String.format("%s\t%s",id,sequence);
		} else {
			return String.format("%s\t%s\t%s",id,sequence,quality);			
		}
	}
}
