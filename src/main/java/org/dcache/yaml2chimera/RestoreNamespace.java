package org.dcache.yaml2chimera;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.google.common.base.Splitter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileExistsChimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsFactory;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.posix.Stat;

public class RestoreNamespace {

    private static class FileRecord {

        public String path;
        public String pnfsid;
        public String csum;
        public int uid;
        public int gid;
        public long size;
    }

    public static void main(String[] args) throws FileNotFoundException, YamlException, ChimeraFsException, SQLException, IOException {

        if (args.length != 5) {
            System.err.println("Usage: RestoreNamespace <dump file name> <tsm|yaml> <jdbc-url> <db-user> <db-path>");
            System.exit(1);
        }

        String type = args[1];

        String dbUrl = args[2];
        String dbUser = args[3];
        String dbPass = args[4];

        FileSystemProvider fs = FsFactory.createFileSystem(dbUrl, dbUser, dbPass);

        switch (type) {

            case "yaml": {
                YamlReader reader = new YamlReader(new FileReader(args[0]));
                Map<String, Map<String, Object>> poolInventory = (Map) reader.read();
                for (Map.Entry<String, Map<String, Object>> entry : poolInventory.entrySet()) {

                    Map<String, Object> storageInfo = entry.getValue();

                    Map<String, String> siMap = (Map<String, String>) storageInfo.get("map");
                    String path = siMap.get("path");

                    String state = (String) storageInfo.get("state");
                    if (!state.equals("PRECIOUS")) {
                        continue;
                    }

                    if (path == null) {
                        System.out.println("path is not available: " + entry.getKey());
                        continue;
                    }

                    FileRecord fr = new FileRecord();
                    fr.path = path;
                    fr.pnfsid = entry.getKey();

                    try {
                        fr.uid = Integer.parseInt(siMap.get("uid"));
                    } catch (NumberFormatException e) {
                    }

                    try {
                        fr.gid = Integer.parseInt(siMap.get("gid"));
                    } catch (NumberFormatException e) {
                    }

                    try {
                        fr.size = Long.parseLong((String) storageInfo.get("filesize"));
                    } catch (NumberFormatException e) {
                    }
                    fr.csum = siMap.get("flag-c");

                    createEntry0(fs, fr);
                }
            }
            break;
            case "tsm": {
                BufferedReader r = new BufferedReader(new FileReader(args[0]));
                while (true) {
                    String line = r.readLine();
                    if (line == null) {
                        break;
                    }
                    Splitter splitter = Splitter.on(' ').trimResults();

                    List<String> record = splitter.splitToList(line);
                    System.out.println(record);
                    Map<String, String> siMap = Splitter.on(';')
                            .trimResults()
                            .omitEmptyStrings()
                            .withKeyValueSeparator("=")
                            .split(record.get(7).substring(4));

                    FileRecord fr = new FileRecord();
                    fr.path = siMap.get("path");
                    fr.pnfsid = record.get(5);

                    try {
                        fr.uid = Integer.parseInt(siMap.get("uid"));
                    } catch (NumberFormatException e) {
                    }

                    try {
                        fr.gid = Integer.parseInt(siMap.get("gid"));
                    } catch (NumberFormatException e) {
                    }

                    try {
                        fr.size = Long.parseLong(siMap.get("size"));
                    } catch (NumberFormatException e) {
                    }
                    fr.csum = siMap.get("flag-c");

                    createEntry0(fs, fr);
                }
            }
            break;

            default:
                System.err.println("Unsupported format: " + type);
                System.exit(2);
        }
    }

    static Map<String, FsInode> parentCache = new HashMap<>();

    static FsInode getOrCreateParent(FileSystemProvider fs, String path) throws ChimeraFsException {

        FsInode inode = parentCache.get(path);
        if (inode != null) {
            return inode;
        }

        inode = fs.path2inode("/");

        Splitter splitter = Splitter.on('/').omitEmptyStrings();
        List<String> pathElements = splitter.splitToList(path);
        for (String pe : pathElements) {
            try {
                inode = fs.mkdir(inode, pe, 0, 0, 0755);
            } catch (FileExistsChimeraFsException e) {
                inode = fs.inodeOf(inode, pe, FileSystemProvider.StatCacheOption.NO_STAT);
            }
        }

        parentCache.putIfAbsent(path, inode);
        return inode;
    }

    static void createEntry0(FileSystemProvider fs, FileRecord fr) throws ChimeraFsException {
        System.out.println(
                String.format("create: %s -> %s %d %d %d %s", fr.pnfsid, fr.path, fr.uid, fr.gid, fr.size, fr.csum)
        );
        createEntry(fs, fr);
    }

    static void createEntry(FileSystemProvider fs, FileRecord fr) throws ChimeraFsException {

        File f = new File(fr.path);
        File parent = f.getParentFile();

        FsInode pInode = getOrCreateParent(fs, parent.getAbsolutePath());
        fs.createFileWithId(pInode, fr.pnfsid, f.getName(), fr.uid, fr.gid, 0644, UnixPermission.S_IFREG);
        FsInode inode = fs.id2inode(fr.pnfsid, FileSystemProvider.StatCacheOption.NO_STAT);
        Stat stat = new Stat();
        stat.setSize(fr.size);
        fs.setInodeAttributes(inode, 0, stat);

        if (fr.csum != null && !fr.csum.isEmpty()) {
            String[] s = fr.csum.split(":");
            if (s.length == 2) {
                fs.setInodeChecksum(inode, Integer.parseInt(s[0]), s[1]);
            }
        }
    }

}
