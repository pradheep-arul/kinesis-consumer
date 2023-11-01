package com.pradheeparul.utils;

/**
 * Constants
 *
 * @author PradheepKumarA
 */
public class Constants {


    // Name of the Kinesis stream
    public static final String STREAM_NAME = PropertyUtils.getProperty("kinesis", "kinesis_stream");

    // Maximum number of records to fetch from Kinesis
    public static final int RECORDS_FETCH_LIMIT = PropertyUtils.getIntegerProperty("kinesis", "kinesis_records_fetch_limit");

}
