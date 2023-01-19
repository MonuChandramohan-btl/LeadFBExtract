package com.blueteak.csvutil;

import java.io.File;

import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;

public class CSVHelper {

	public  CellProcessor[] getProcessors() {
		final CellProcessor[] processors = new CellProcessor[] {
				new NotNull(new ParseInt()),
				new NotNull(), 
				new NotNull(new ParseInt()),
				new NotNull(), 
				new NotNull()
		};
		return processors;
	}

	/**
	 * @return
	 */
	public String getAbsPath() {
		String path = "src/main/resources/combined";
		File file = new File(path);
		String absolutePath = file.getAbsolutePath();
		System.out.println(absolutePath);
		return absolutePath;
	}
}
