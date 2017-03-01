package org.dcache.yaml2chimera;

import com.esotericsoftware.yamlbeans.YamlReader;
import com.google.common.base.Splitter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileExistsChimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;

public class RestoreNamespace {

    private static class FileRecord {

        public String path;
        public String pnfsid;
        public String csum;
        public int uid;
        public int gid;
        public long size;
        public String sGroup;
        public String hsm;
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.err.println("Usage: RestoreNamespace <dump file name> <tsm|yaml> <jdbc-url> <db-user> <db-path>");
            System.exit(1);
        }

        String type = args[1];

        String dbUrl = args[2];
        String dbUser = args[3];
        String dbPass = args[4];


        String dbDrv;
        String dialect;
        String[] s = dbUrl.split(":");

        switch(s[1]) {
            case "h2":
                dbDrv = "org.h2.Driver";
                dialect = "H2";
                break;
            case "postgresql":
                dbDrv = "org.postgresql.Driver";
                dialect = "PgSQL";
                break;
            default:
                throw new IllegalArgumentException("Unsuported db url: " + dbUrl);
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbUrl);
        config.setUsername(dbUser);
        config.setPassword(dbPass);
        config.setDriverClassName(dbDrv);
        config.setMaximumPoolSize(3);

        DataSource dataSource = new HikariDataSource(config);

        try (Connection conn = dataSource.getConnection()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(conn));
            Liquibase liquibase = new Liquibase("org/dcache/chimera/changelog/changelog-master.xml",
                    new ClassLoaderResourceAccessor(), database);
            liquibase.update("");
        }

        JdbcFs fs = new JdbcFs(dataSource, dialect);

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
                    // as we filter PRECIUOS files only, ignore storage class as it recorded on flush
                    //fr.sGroup = (String)storageInfo.get("storageclass");

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
                    if (record.size() < 8 || !record.get(7).startsWith("-si=")) {
                        System.err.println("Skip invalid record: " + record);
                        continue;
                    }

                    Map<String, String> siMap = Splitter.on(';')
                            .trimResults()
                            .omitEmptyStrings()
                            .withKeyValueSeparator("=")
                            .split(record.get(7).substring(4));

                    FileRecord fr = new FileRecord();
                    fr.path = siMap.get("path");
                    fr.pnfsid = record.get(5);
                    fr.sGroup = siMap.get("sClass");
                    fr.hsm = siMap.get("hsm");

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

    static FsInode getOrCreateParent(JdbcFs fs, String path) throws ChimeraFsException {

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
                inode = fs.inodeOf(inode, pe);
            }
        }

        parentCache.putIfAbsent(path, inode);
        return inode;
    }

    static void createEntry0(JdbcFs fs, FileRecord fr) throws ChimeraFsException {
        System.out.println(
                String.format("create: %s -> %s %d %d %d %s", fr.pnfsid, fr.path, fr.uid, fr.gid, fr.size, fr.csum)
        );
        createEntry(fs, fr);
    }

    static void createEntry(JdbcFs fs, FileRecord fr) throws ChimeraFsException {

        if (fr.path == null) {
            System.err.println("No path for: " + fr.pnfsid);
            return;
        }

        File f = new File(fr.path);
        File parent = f.getParentFile();

        FsInode pInode = getOrCreateParent(fs, parent.getAbsolutePath());
        FsInode inode = new FsInode(fs, fr.pnfsid);
        if (inode.exists()) {
            // duplicate entry
            return;
        }

        try {
            fs.createFileWithId(pInode, inode, f.getName(), fr.uid, fr.gid, 0644, UnixPermission.S_IFREG);
        }catch (FileExistsChimeraFsException e) {
            System.err.println("Conflicting name: " + f.getName());
            // name conflict. can be due to partial upload on to multiple pools
            fs.createFileWithId(pInode, inode, f.getName() + "_conflict_" + fr.pnfsid, fr.uid, fr.gid, 0644, UnixPermission.S_IFREG);
        }

        Stat stat = new Stat();
        stat.setSize(fr.size);
        fs.setInodeAttributes(inode, 0, stat);
        if (fr.sGroup != null) {
            String[] s = fr.sGroup.split(":");
            InodeStorageInformation si = new InodeStorageInformation(inode, fr.hsm, s[0], s[1]);
            fs.setStorageInfo(inode, si);
            fs.addInodeLocation(inode, 0,
                    String.format("osm://lofar.psnc.pl/?store=%s&group=%s&bfid=%s", s[0], s[1], fr.pnfsid)
                    );
        }

        if (fr.csum != null && !fr.csum.isEmpty()) {
            String[] s = fr.csum.split(":");
            if (s.length == 2) {
                fs.setInodeChecksum(inode, Integer.parseInt(s[0]), s[1]);
            }
        }
    }

}
