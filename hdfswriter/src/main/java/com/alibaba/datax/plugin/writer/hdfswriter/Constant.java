package com.alibaba.datax.plugin.writer.hdfswriter;

import java.util.List;

import com.google.common.collect.ImmutableList;

public class Constant {

	public static final String DEFAULT_ENCODING = "UTF-8";
	public static final String DEFAULT_NULL_FORMAT = "\\N";
	
	public final static String WRITE_MODE_APPEND="append";
	public final static String WRITE_MODE_NONCONFLICT="nonconflict";
	public final static String WRITE_MODE_TRUNCATE="truncate";
	
    public final static List<String> WRITEMODE_LISTS = ImmutableList.of(
    		WRITE_MODE_APPEND, WRITE_MODE_NONCONFLICT, WRITE_MODE_TRUNCATE);
	
}
