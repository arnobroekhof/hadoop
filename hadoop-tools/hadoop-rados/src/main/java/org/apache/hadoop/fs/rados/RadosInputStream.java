package org.apache.hadoop.fs.rados;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ceph.rados.IoCTX;

public class RadosInputStream extends InputStream {

    private static final Log LOG =
        LogFactory.getLog(RadosInputStream.class.getName());
    private static IoCTX ioctx;
    private boolean closed;
    private long size = -1;
    private long pos = 0;
    private String oid;

    public RadosInputStream(IoCTX io, String id) {
        ioctx = io;
        oid = id;
        closed = false;
    }

    public RadosInputStream(RadosFileSystemStore store, String id) {
        ioctx = store.getIoCTX();
        oid = id;
        closed = false;
    }

    private synchronized long getPos() throws IOException {
        return pos;
    }

    @Override
    public synchronized int available() throws IOException {
        try {
            if (size < 0) {
                size = ioctx.stat(oid).getSize();
            }
            return (int) (size - pos);
        }
        catch (Exception e) {
            throw new IOException("stat failed");
        }
    }

    @Override
    public synchronized int read() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        try {
            if (size < 0) {
                size = ioctx.stat(oid).getSize();
            }
        }
        catch (Exception e) {
            throw new IOException("stat failed");
        }
        byte[] buf = new byte[1];
        try {
            int read = ioctx.read(oid, 1, pos, buf);
            if (read > 0)
                pos++;
            return buf[0];
        }
        catch (Exception e) {
            throw new IOException("read failed");
        }
    }

    @Override
    public synchronized int read(byte buf[], int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        try {
            byte[] b = new byte[len];
            int read = ioctx.read(oid, len, pos, b);
            if (read > 0) {
                pos += read;
            }
            System.arraycopy(b, 0, buf, off, read);
            return read;
        }
        catch (Exception e) {
            throw new IOException("read failed");
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

    /**
     * We don't support marks.
     */
    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readLimit) {
        // Do nothing
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("Mark not supported");
    }
}
