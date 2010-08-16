package com.lifetech.hadoop.mapreduce.input;

public class Sequence {

	private String id;
	private String sequence = "";

	public Sequence(byte[] data,int length) {
		parse(new String(data,0,length));
	}

	@Override
	public String toString() {
		return String.format("%s\t%s", id,sequence);
	}

	private void parse(String seq) {
		String[] l = seq.split("\n");
		id = l[0];
		for (int i = 1; i < l.length; i++)
			sequence = sequence + l[i].replace("\n", "");
		// Pattern pattern = Pattern.compile(">\\s?(.*)\\s?(.*)");
		// Matcher matcher = pattern.matcher(seq);
		// if (matcher.matches()) {
		// id = matcher.group(1);
		// sequence = matcher.group(2).replace("\n", "");
		// }
	}

}
