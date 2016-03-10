package org.apache.hadoop.fs.rados;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * A block-based {@link FileSystem} backed by Ceph Rados
 * </p>
 */
public class RadosFileSystem extends FileSystem {

    private URI uri;

    private RadosFileSystemStore store;

    private Path workingDir;

    public RadosFileSystem() {
        // set store in initialize()
    }

    public RadosFileSystem(RadosFileSystemStore store) {
        this.store = store;
    }

    private static RadosFileSystemStore createDefaultStore() {
        return new RadosFileSystemStore();
    }

    /**
     * Return the protocol scheme for the FileSystem.
     * <p/>
     *
     * @return <code>rados</code>
     */
    @Override
    public String getScheme() {
        return "rados";
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        if (store == null) {
            String conf_file = conf.get("ceph_conf");
            String id = conf.get("ceph_id");
            String pool = conf.get("ceph_pool");

            store = createDefaultStore();
            store.initialize(conf_file, id, pool);
        }

        setConf(conf);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir =
            new Path("/user", System.getProperty("user.name")).makeQualified(this);
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public void setWorkingDirectory(Path dir) {
        workingDir = makeAbsolute(dir);
    }

    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDir, path);
    }

    /**
     * @param permission Currently ignored.
     */
    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        Path absolutePath = makeAbsolute(path);
        List<Path> paths = new ArrayList<Path>();
        do {
            paths.add(0, absolutePath);
            absolutePath = absolutePath.getParent();
        } while (absolutePath != null);

        boolean result = true;
        for (Path p : paths) {
            result &= mkdir(p);
        }
        return result;
    }

    private boolean mkdir(Path path) throws IOException {
        Path absolutePath = makeAbsolute(path);
        INode inode = store.retrieveINode(absolutePath);
        if (inode == null) {
            store.storeINode(absolutePath, INode.DIRECTORY_INODE);
        }
        else if (inode.isFile()) {
            throw new IOException(String.format(
                "Can't make directory for path %s since it is a file.",
                absolutePath));
        }
        return true;
    }

    @Override
    public boolean isFile(Path path) throws IOException {
        INode inode = store.retrieveINode(makeAbsolute(path));
        if (inode == null) {
            return false;
        }
        return inode.isFile();
    }

    private INode checkFile(Path path) throws IOException {
        INode inode = store.retrieveINode(makeAbsolute(path));
        if (inode == null) {
            throw new IOException("No such file.");
        }
        if (inode.isDirectory()) {
            throw new IOException("Path " + path + " is a directory.");
        }
        return inode;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        Path absolutePath = makeAbsolute(f);
        INode inode = store.retrieveINode(absolutePath);
        if (inode == null) {
            throw new FileNotFoundException("File " + f + " does not exist.");
        }
        if (inode.isFile()) {
            return new FileStatus[]{
                new RadosFileStatus(f.makeQualified(this), inode)
            };
        }
        ArrayList<FileStatus> ret = new ArrayList<FileStatus>();
        for (Path p : store.listSubPaths(absolutePath)) {
            ret.add(getFileStatus(p.makeQualified(this)));
        }
        return ret.toArray(new FileStatus[0]);
    }

    /**
     * This optional operation is not yet supported.
     */
    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
                                     Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }

    /**
     * @param permission Currently ignored.
     */
    @Override
    public FSDataOutputStream create(Path file, FsPermission permission,
                                     boolean overwrite, int bufferSize,
                                     short replication, long blockSize, Progressable progress)
        throws IOException {

        INode inode = store.retrieveINode(makeAbsolute(file));
        if (inode != null) {
            if (overwrite) {
                delete(file, true);
            }
            else {
                throw new FileAlreadyExistsException("File already exists: " + file);
            }
        }
        else {
            Path parent = file.getParent();
            if (parent != null) {
                if (!mkdirs(parent)) {
                    throw new IOException("Mkdirs failed to create " + parent.toString());
                }
            }
        }
        return new FSDataOutputStream
            (new RadosOutputStream(store, makeAbsolute(file).toString()));
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        return new FSDataInputStream(new RadosInputStream(store, path.toString()));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        Path absoluteSrc = makeAbsolute(src);
        final String debugPreamble = "Renaming '" + src + "' to '" + dst + "' - ";
        INode srcINode = store.retrieveINode(absoluteSrc);
        boolean debugEnabled = LOG.isDebugEnabled();
        if (srcINode == null) {
            // src path doesn't exist
            if (debugEnabled) {
                LOG.debug(debugPreamble + "returning false as src does not exist");
            }
            return false;
        }

        Path absoluteDst = makeAbsolute(dst);

        //validate the parent dir of the destination
        Path dstParent = absoluteDst.getParent();
        if (dstParent != null) {
            //if the dst parent is not root, make sure it exists
            INode dstParentINode = store.retrieveINode(dstParent);
            if (dstParentINode == null) {
                // dst parent doesn't exist
                if (debugEnabled) {
                    LOG.debug(debugPreamble +
                        "returning false as dst parent does not exist");
                }
                return false;
            }
            if (dstParentINode.isFile()) {
                // dst parent exists but is a file
                if (debugEnabled) {
                    LOG.debug(debugPreamble +
                        "returning false as dst parent exists and is a file");
                }
                return false;
            }
        }

        //get status of source
        boolean srcIsFile = srcINode.isFile();

        INode dstINode = store.retrieveINode(absoluteDst);
        boolean destExists = dstINode != null;
        boolean destIsDir = destExists && !dstINode.isFile();
        if (srcIsFile) {

            //source is a simple file
            if (destExists) {
                if (destIsDir) {
                    //outcome #1 dest exists and is dir -filename to subdir of dest
                    if (debugEnabled) {
                        LOG.debug(debugPreamble +
                            "copying src file under dest dir to " + absoluteDst);
                    }
                    absoluteDst = new Path(absoluteDst, absoluteSrc.getName());
                }
                else {
                    //outcome #2 dest it's a file: fail iff different from src
                    boolean renamingOnToSelf = absoluteSrc.equals(absoluteDst);
                    if (debugEnabled) {
                        LOG.debug(debugPreamble +
                            "copying file onto file, outcome is " + renamingOnToSelf);
                    }
                    return renamingOnToSelf;
                }
            }
            else {
                // #3 dest does not exist: use dest as path for rename
                if (debugEnabled) {
                    LOG.debug(debugPreamble +
                        "copying file onto file");
                }
            }
        }
        else {
            //here the source exists and is a directory
            // outcomes (given we know the parent dir exists if we get this far)
            // #1 destination is a file: fail
            // #2 destination is a directory: create a new dir under that one
            // #3 destination doesn't exist: create a new dir with that name
            // #3 and #4 are only allowed if the dest path is not == or under src

            if (destExists) {
                if (!destIsDir) {
                    // #1 destination is a file: fail
                    if (debugEnabled) {
                        LOG.debug(debugPreamble +
                            "returning false as src is a directory, but not dest");
                    }
                    return false;
                }
                else {
                    // the destination dir exists
                    // destination for rename becomes a subdir of the target name
                    absoluteDst = new Path(absoluteDst, absoluteSrc.getName());
                    if (debugEnabled) {
                        LOG.debug(debugPreamble +
                            "copying src dir under dest dir to " + absoluteDst);
                    }
                }
            }
            //the final destination directory is now know, so validate it for
            //illegal moves

            if (absoluteSrc.equals(absoluteDst)) {
                //you can't rename a directory onto itself
                if (debugEnabled) {
                    LOG.debug(debugPreamble +
                        "Dest==source && isDir -failing");
                }
                return false;
            }
            if (absoluteDst.toString().startsWith(absoluteSrc.toString() + "/")) {
                //you can't move a directory under itself
                if (debugEnabled) {
                    LOG.debug(debugPreamble +
                        "dst is equal to or under src dir -failing");
                }
                return false;
            }
        }
        //here the dest path is set up -so rename
        return renameRecursive(absoluteSrc, absoluteDst);
    }

    private boolean renameRecursive(Path src, Path dst) throws IOException {
        INode srcINode = store.retrieveINode(src);
        store.storeINode(dst, srcINode);
        store.deleteINode(src);
        if (srcINode.isDirectory()) {
            for (Path oldSrc : store.listDeepSubPaths(src)) {
                INode inode = store.retrieveINode(oldSrc);
                if (inode == null) {
                    return false;
                }
                String oldSrcPath = oldSrc.toUri().getPath();
                String srcPath = src.toUri().getPath();
                String dstPath = dst.toUri().getPath();
                Path newDst = new Path(oldSrcPath.replaceFirst(srcPath, dstPath));
                store.storeINode(newDst, inode);
                store.deleteINode(oldSrc);
            }
        }
        return true;
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        Path absolutePath = makeAbsolute(path);
        INode inode = store.retrieveINode(absolutePath);
        if (inode == null) {
            return false;
        }
        if (inode.isFile()) {
            store.deleteINode(absolutePath);
            for (Block block : inode.getBlocks()) {
                store.deleteBlock(block);
            }
        }
        else {
            FileStatus[] contents = null;
            try {
                contents = listStatus(absolutePath);
            }
            catch (FileNotFoundException fnfe) {
                return false;
            }

            if ((contents.length != 0) && (!recursive)) {
                throw new IOException("Directory " + path.toString()
                    + " is not empty.");
            }
            for (FileStatus p : contents) {
                if (!delete(p.getPath(), recursive)) {
                    return false;
                }
            }
            store.deleteINode(absolutePath);
        }
        return true;
    }

    /**
     * FileStatus for S3 file systems.
     */
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        INode inode = store.retrieveINode(makeAbsolute(f));
        if (inode == null) {
            throw new FileNotFoundException(f + ": No such file or directory.");
        }
        return new RadosFileStatus(f.makeQualified(this), inode);
    }

    @Override
    public long getDefaultBlockSize() {
        return getConf().getLong("fs.s3.block.size", 64 * 1024 * 1024);
    }

    @Override
    public String getCanonicalServiceName() {
        // Does not support Token
        return null;
    }

    // diagnostic methods

    void dump() throws IOException {
        store.dump();
    }

    void purge() throws IOException {
        store.purge();
    }

    private static class RadosFileStatus extends FileStatus {

        RadosFileStatus(Path f, INode inode) throws IOException {
            super(findLength(inode), inode.isDirectory(), 1,
                findBlocksize(inode), 0, f);
        }

        private static long findLength(INode inode) {
            if (!inode.isDirectory()) {
                long length = 0L;
                for (Block block : inode.getBlocks()) {
                    length += block.getLength();
                }
                return length;
            }
            return 0;
        }

        private static long findBlocksize(INode inode) {
            final Block[] ret = inode.getBlocks();
            return ret == null ? 0L : ret[0].getLength();
        }
    }
}
