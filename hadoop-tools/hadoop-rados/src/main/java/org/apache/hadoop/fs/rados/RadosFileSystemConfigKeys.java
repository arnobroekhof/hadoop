package org.apache.hadoop.fs.rados;

import org.apache.hadoop.fs.CommonConfigurationKeys;

/**
 * This class contains constants for configuration keys used
 * in the rados file system.
 *
 */
public class RadosFileSystemConfigKeys extends CommonConfigurationKeys {
    public static final String  S3_BLOCK_SIZE_KEY = "s3.blocksize";
    public static final long    S3_BLOCK_SIZE_DEFAULT = 64*1024*1024;
    public static final String  S3_REPLICATION_KEY = "s3.replication";
    public static final short   S3_REPLICATION_DEFAULT = 1;
    public static final String  S3_STREAM_BUFFER_SIZE_KEY =
        "s3.stream-buffer-size";
    public static final int     S3_STREAM_BUFFER_SIZE_DEFAULT = 4096;
    public static final String  S3_BYTES_PER_CHECKSUM_KEY =
        "s3.bytes-per-checksum";
    public static final int     S3_BYTES_PER_CHECKSUM_DEFAULT = 512;
    public static final String  S3_CLIENT_WRITE_PACKET_SIZE_KEY =
        "s3.client-write-packet-size";
    public static final int     S3_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64*1024;
}
