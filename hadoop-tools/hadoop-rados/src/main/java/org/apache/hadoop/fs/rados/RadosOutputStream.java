package org.apache.hadoop.fs.rados;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ceph.rados.IoCTX;

public class RadosOutputStream extends OutputStream {

    private static final Log LOG =
        LogFactory.getLog(RadosOutputStream.class.getName());
    private static IoCTX ioctx;
    private boolean closed;
    private String oid;
    private long pos = 0;

    public RadosOutputStream(IoCTX io, String id) {
        ioctx = io;
        oid = id;
        closed = false;
    }

    public RadosOutputStream(RadosFileSystemStore store, String id) {
        ioctx = store.getIoCTX();
        oid = id;
        closed = false;
    }

    @Override
    public synchronized void write(int b) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        byte[] buf = new byte[4];
        try {
            ioctx.write(oid, buf, pos);
            pos++;
        }
        catch (Exception e) {
            throw new IOException("write failed");
        }
    }

    @Override
    public synchronized void write(byte buf[], int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        try {
            ioctx.write(oid, buf, off);
            pos += buf.length;
        }
        catch (Exception e) {
            throw new IOException("write failed");
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        super.close();
        closed = true;
    }
}
