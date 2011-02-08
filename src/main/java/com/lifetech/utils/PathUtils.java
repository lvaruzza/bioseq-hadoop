package com.lifetech.utils;

import java.io.File;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;

public class PathUtils {
	public static Path changePathExtension(Path orig,String newExtension) {
		String name = orig.getParent().toString() + '/'  +
					  FilenameUtils.getBaseName(orig.getName()) + newExtension;

		return new Path(name);
	}
}
