package txt2db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Txt2db {

    public String co = "(c)SJFN@ txt2db v[140928] by foxz free.fr. CC:ByNC";

    private enum dbmethode {

        UpDate, Add, Merge
    };

    private enum direction {

        dirHorz, dirVert
    };

    private String path;
    private List<String> lstFiles = new ArrayList<>();

    private void getFiles(String mask) {
        if (!mask.contains("*")) {
            lstFiles.add(mask);
        } else {
            try {
                System.out.println("Multi-file processing");
                int y = mask.lastIndexOf("/");
                if (y == -1) {
                    y = 0;
                }
                File tf = new File(mask.substring(0, y));
                path = tf.getCanonicalPath().concat("/");
                y = mask.lastIndexOf(".");
                final String ext = mask.substring(y, mask.length());
                FilenameFilter ff = new FilenameFilter() {
                    @Override
                    public boolean accept(File file, String string) {
                        return string.endsWith(ext);
                    }
                };
                lstFiles = Arrays.asList(tf.list(ff));
            } catch (IOException ex) {
                System.err.println("--- config 'File=' error");
                System.exit(1);
            }
        }

    }

    class strucs {

        String name;
        String defaut;
        boolean nullable;

        public strucs(String n, boolean b, String d) {
            name = n;
            nullable = b;
            defaut = d;
        }
    }

    private List<strucs> struc = new ArrayList<>();

    private void getStruct(String table) throws SQLException {
        struc.clear();
        DatabaseMetaData mt = con.getMetaData();
        ResultSet rs = mt.getColumns(null, null, table, null);
        while (rs.next()) {
            struc.add(new strucs(rs.getString("COLUMN_NAME"), rs.getBoolean("NULLABLE"), rs.getString("COLUMN_DEF")));
        }

    }

    private Map<String, String> data = new HashMap<>();

    private void mkData() {
        data.clear();
        //todo: alias
        for (strucs f : struc) {
            data.put(f.name, f.defaut);
        }
    }

    private void clrData() {
        mkData();
    }

    private Connection con;

    private PreparedStatement psUpDate;
    private PreparedStatement psAdd;
    private PreparedStatement psMasterKey;

    private String masterKey;

    private void prepareSQLUpDate(String table) throws SQLException {
        String t1 = "";
        for (String f : data.keySet()) {
            if (!t1.isEmpty()) {
                t1 = t1.concat(",");
            }
            t1 = t1.concat(f).concat("=?");
        }
        psUpDate = con.prepareStatement(String.format("update %s set %s where %s=?", table, t1, masterKey));
    }

    private void prepareSQLAdd(String table) throws SQLException {
        String sql;
        String t1 = "";
        String t2 = "";
        for (String f : data.keySet()) {
            if (!t1.isEmpty()) {
                t1 = t1.concat(",");
                t2 = t2.concat(",");
            }
            t1 = t1.concat(f);
            t2 = t2.concat("?");
        }
        psAdd = con.prepareStatement(String.format("insert into %s (%s) values (%s)", table, t1, t2));
    }

    private void prepareSqlMasterKey(String table) throws SQLException {
        String dbg = masterKey;
        psMasterKey = con.prepareStatement(String.format("select count(*) as nb from %s where %s=?", table, masterKey));
    }

    public void run(String[] args) throws IOException, SQLException {
        String triggerRecord = null;

        String triggerField;

        String fileMask;
        BufferedReader rdr;
        int skip;

        List<String> FieldName = null;
        boolean header = false;
        String trsD = null;
        int triD = 0;

        String masterKeyValue = null;

        Properties prop = new Properties();

        // <editor-fold desc="--- load config">
        System.out.println(String.format("Loading config : %s", args[0]));
        prop.load(new FileReader(args[0]));
        // </editor-fold>
        //<editor-fold desc="--- parameter">
        if (args.length > 1) {
            System.out.println(String.format("Assuming commandline : %s", args[1]));
            fileMask = args[1];
        } else {
            System.out.println("Assuming config filemask");
            fileMask = prop.getProperty("File", "*.txt");
        }
        getFiles(fileMask);
        //</editor-fold>
        //<editor-fold desc="--- jdbc init">
        String host = String.format("jdbc:%s://%s/%s", prop.getProperty("Driver"), prop.getProperty("Host"), prop.getProperty("Base"));
        System.out.println("JDBC Connexion");
        con = DriverManager.getConnection(host, prop.getProperty("Login"), prop.getProperty("Pwd"));
        String table = prop.getProperty("Table");
        //</editor-fold>
        //<editor-fold desc="--- methode update/add">
        //<editor-fold desc="--- file type selection Horz/Vert">
        triggerField = prop.getProperty("Field", ";");
        switch (triggerField) {
            case "\\t":
            case "/t": // tabbed
                triggerField = "\t";
                break;
        }
        //</editor-fold>
        //<editor-fold desc="--- direction">
        direction dir = null;
        switch (prop.getProperty("Type", "Horizontal")) {
            case "h":
            case "H":
            case "Horizontal":
            case "Line":
                System.out.println("assuming horizontal");
                dir = direction.dirHorz;
                break;
            case "v":
            case "V":
            case "Verticale":
                System.out.print("Assuming Verticale file Type");
                dir = direction.dirVert;
                triggerRecord = prop.getProperty("Record", ".");
                switch (triggerRecord.charAt(0)) {
                    case '#': // x line par record (const)
                        triD = Integer.parseInt(triggerRecord.substring(3, triggerRecord.length()));
                        System.out.println(String.format("with %d/record", triD));
                        break;
                    case '*': // record ended by named field
                        trsD = triggerField;
                        System.out.println(String.format("with field '%s' for last", trsD));
                        break;
                    case '.': // record ended by a sequence
                        trsD = triggerRecord.substring(1, triggerRecord.length());
                        if (trsD.isEmpty()) {
                            System.out.println(String.format("with '%s' at last", trsD));
                        } else {
                            System.out.println("with blank at last");
                        }
                        break;
                    case '1':
                        System.out.println(String.format(" 1 record/file", trsD));
                        break;
                    default:
                        System.err.println("--- config file 'Record=' error");
                        System.exit(1);
                }
        }
        //</editor-fold>
        //<editor-fold desc="--- header present ?">        
        switch (prop.getProperty("Header", "").toLowerCase()) {
            case "true":
                header = true;
                switch (dir) {
                    case dirHorz:
                        System.out.println("assuming header");
                        FieldName = new ArrayList<>();
                        break;
                    case dirVert:
                        System.out.println(String.format("each line begin with fieldname'%s'", triggerField));
                        break;
                }
                break;
            case "false":
                break;
        }
        //</editor-fold>
        //<editor-fold desc="--- skip ?">
        skip = Integer.parseInt(prop.getProperty("Skip", "0"));
        //</editor-fold>        

        dbmethode methode = dbmethode.valueOf(prop.getProperty("Methode", "Add"));
        masterKey = prop.getProperty("MasterKey").toLowerCase();

        //todo: {table} var
        getStruct(table);
        methode = dbmethode.valueOf(prop.getProperty("Methode"));
        switch (methode) {
            case Add:
                prepareSQLAdd(table);
                break;
            case UpDate:
                if (masterKey.isEmpty()) {
                    System.err.println("--- config 'MasterKey' must set");
                    System.exit(2);
                }
                prepareSQLUpDate(table);
                break;
            case Merge:
                System.out.println(String.format("Merge with '%s' for masterKey", masterKey));
                if (masterKey.isEmpty()) {
                    System.err.println("--- config 'MasterKey' must set");
                    System.exit(2);
                }
                mkData();
                prepareSQLUpDate(table);
                prepareSQLAdd(table);
                prepareSqlMasterKey(table);
                break;
        }
        // proceed each file

        for (String curfile : lstFiles) {
            System.out.print(String.format("proced : %s ... ", curfile));
            rdr = new BufferedReader(new FileReader(path.concat(curfile)));
            int ln = 0;

            //<editor-fold desc="--- skipping">
            if (skip > 0) {
                System.out.print(String.format("Skipping %d line(s) ... ", skip));
                for (int n = 0; n < skip; n++) {
                    rdr.readLine();
                    ln++;
                }
            }
            //</editor-fold>            
            //<editor-fold desc="--- read header if Horz">
            if (header && dir == direction.dirHorz) {
                System.out.print("read header ... ");
                FieldName = Arrays.asList(rdr.readLine().split(triggerField));
                //todo: check conform
                ln++;
            }
            //</editor-fold>
            // proceed 

            int seq = 0;
            boolean push2db;
            String fn = "";
            while (rdr.ready()) {
                String l = rdr.readLine();
                push2db = !rdr.ready();
                ln++;
                //<editor-fold desc="--- handle line">
                switch (dir) {
                    case dirHorz:
                        //<editor-fold desc="--- Horz->data">
                        String[] spf = l.split(triggerField);
                        int n = 0;
                        for (String tfn : FieldName) {
                            data.put(tfn, spf[n++]);
                        }
                        push2db = true;
                        break;
                    //</editor-fold>
                    case dirVert:
                        if (header) {
                            //<editor-fold desc="--- Vert split field<triggerField>data">
                            int y = l.indexOf(triggerField);
                            fn = l.substring(0, y).toLowerCase();
                            String d = l.substring(y + 1, l.length()).trim();
                            data.put(fn, d);
                            //</editor-fold>
                        } else {
                            data.put(FieldName.get(seq++), l);
                        }
                        switch (triggerRecord) {
                            case "#":
                                if (seq == triD) {
                                    push2db = true;
                                }
                                break;
                            case "*":
                                if (fn.equals(trsD)) {
                                    push2db = true;
                                }
                                break;
                            case ".":
                                if (l.equals(trsD)) {
                                    push2db = true;
                                }
                                break;
                        }
                }
                //</editor-fold>
                //<editor-fold desc="--- update/insert into db">                
                if (push2db) {
                    dbmethode om = methode;
                    seq = 0;
                    push2db = false;
                    int n = 1;
                    if (methode == dbmethode.Merge) {
                        masterKeyValue = data.get(masterKey).trim();
                        psMasterKey.setString(1, masterKeyValue);
                        //getrow suck sometime
                        ResultSet rs = psMasterKey.executeQuery();
                        rs.first();
                        int i = rs.getInt(1);
                        rs.close();
                        if (i > 0) {
                            methode = dbmethode.UpDate;
                        } else {
                            methode = dbmethode.Add;
                        }
                    }
                    switch (methode) {
                        case Add:
                            for (String v : data.values()) {
                                psAdd.setString(n++, v);
                            }
                            psAdd.executeUpdate();
                            break;
                        case UpDate:
                            masterKeyValue = data.get(masterKey);
                            for (String v : data.values()) {
                                psUpDate.setString(n++, v);
                            }
                            psUpDate.setString(n++, masterKeyValue);
                            psUpDate.executeUpdate();
                            break;
                    }
                    methode = om;
                    clrData();
                }
                //</editor-fold>
            }
            System.out.println("... OK");
        }
        //con.commit();
        if (psAdd != null) {
            psAdd.close();
        }
        if (psUpDate != null) {
            psUpDate.close();
        }
        con.close();
    }

    public static void main(String[] args) throws IOException, SQLException {
        Txt2db r = new Txt2db();
        r.run(args);
    }

}
