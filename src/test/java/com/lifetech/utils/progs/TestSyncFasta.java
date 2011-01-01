package com.lifetech.utils.progs;

import org.junit.Test;

public class TestSyncFasta {

	@Test
	public void testExecute() throws Exception {
		SyncFasta prog = new SyncFasta();
		prog.execute("data/testsync/a.fasta", "data/testsync/b.qual","data/testsync/a.qual");
	}
}
