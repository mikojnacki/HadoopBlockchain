package com.mikolaj.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Mikolaj on 30.06.17.
 */
public class MyUtils {

    public static String getCurrentDateTime() {
        LocalDateTime currentDateTime = LocalDateTime.now();
        return currentDateTime.toString();
    }

    public static File[] readFiles(String path) {
        File folder = new File(path);
        return folder.listFiles();
    }

    public static Parsed parse(Block block, long gid) {

        NetworkParameters np = new MainNetParams();
        int txMultiplier = 10000; // 10^4
        int txinMultiplier = 10000; // 10^4
        int txoutMultiplier = 100000; // 10^5

        // Blocks
        List<Blk> blks = new ArrayList<Blk>();
        long idBlk = gid;
        String blockHash;
        String prevBlockHash;
        long time;
        int blockSize;

        // Transactions
        List<Tx> txs = new ArrayList<Tx>();
        long idTx = txMultiplier * gid;
        String transactionHash;
        int transactionSize;
        boolean coinbase;
        long transactionOutValue;
        long blk_id; //FK

        // TransactionInputs
        List<Txin> txins = new ArrayList<Txin>();
        long idTxin = txinMultiplier * gid;
        int txinIndex;
        String prevTransactionHash;
        long prevTransactionIndex;
        long tx_id; //FK

        // TransactionOutputs
        List<Txout> txouts = new ArrayList<Txout>();
        long idTxout = txoutMultiplier * gid;
        int txoutIndex;
        String outAddress;
        long value;
        String typeStr;
        // FK tx_id the same as for TransactionInputs

        //Blk
        blockHash = block.getHashAsString();
        prevBlockHash = block.getPrevBlockHash().toString();
        //time = block.getTime().getTime();
        time = block.getTimeSeconds();
        blockSize = block.getMessageSize();
        Blk blk = new Blk(idBlk, blockHash, prevBlockHash, time, blockSize);
        blks.add(blk);

        //Tx
        for(Transaction transaction : block.getTransactions()) {
            //set variables
            transactionHash = transaction.getHashAsString();
            transactionSize = transaction.getMessageSize();
            coinbase = transaction.isCoinBase();
            transactionOutValue = transaction.getOutputSum().value;
            blk_id = idBlk;
            Tx tx = new Tx(idTx, transactionHash, transactionSize, coinbase, transactionOutValue, blk_id);
            txs.add(tx);

            //Txin
            txinIndex = 0; //set to 0
            tx_id = idTx;
            for(TransactionInput transactionInput : transaction.getInputs()) {
                //set variables
                prevTransactionHash = transactionInput.getOutpoint().getHash().toString(); // ok
                prevTransactionIndex = transactionInput.getOutpoint().getIndex(); // not sure

                Txin txin = new Txin(idTxin, txinIndex, prevTransactionHash, prevTransactionIndex, tx_id);
                txins.add(txin);

                txinIndex = txinIndex + 1;
                idTxin = idTxin + 1;
            }

            //Txout
            txoutIndex = 0; // set to 0
            // tx_id = idTx; already set above - the same as for transactionInput
            for(TransactionOutput transactionOutput : transaction.getOutputs()) {
                //set variables
                try {
                    value = transactionOutput.getValue().longValue();
                    Script.ScriptType type = transactionOutput.getScriptPubKey().getScriptType(); // enum
                    typeStr = type.toString();
                    tx_id = idTx;
                    // take output address
                    switch (type) {
                        case P2PKH:
                            outAddress = transactionOutput.getAddressFromP2PKHScript(np).toBase58();
                            break;
                        case P2SH:
                            outAddress = transactionOutput.getAddressFromP2SH(np).toBase58();
                            break;
                        case PUB_KEY:
                            Script outScriptPK = transactionOutput.getScriptPubKey();
                            byte[] byteOutAddressPK = outScriptPK.getPubKey();
                            Address outAddressPK = new Address(MainNetParams.get(), Utils.sha256hash160(byteOutAddressPK));
                            outAddress = outAddressPK.toBase58();
                            break;
                        default: //NO_TYPE and others if exist
                            outAddress = "unknown";
                            break;
                    }
                } catch (ScriptException e) {
                    value = 0;
                    typeStr = "NO_TYPE";
                    outAddress = "unknown";
                    tx_id = idTx;
                    e.printStackTrace();
                }

                Txout txout = new Txout(idTxout, txoutIndex, outAddress, value, typeStr, tx_id);
                txouts.add(txout);

                txoutIndex = txoutIndex + 1;
                idTxout = idTxout + 1;
            }

            idTx = idTx + 1;
        }

        return new Parsed(blks, txs, txins, txouts);
    }

    public static void insertBlocks(List<Blk> blks, Connection conn, Statement stmt) {
        // Blks
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/blockchain",
                            "postgres", "jalokim123");
            conn.setAutoCommit(false);
            //System.out.println("BLK: Database opened successfully");

            // Insert records
            for (Blk blk : blks) {
                stmt = conn.createStatement();
                String sql = "INSERT INTO blk (id, hash, prev_hash, time, blk_size) VALUES ("
                        + blk.getId() + ", "
                        + "'" + blk.getBlockHash() + "', "
                        + "'" + blk.getPrevBlockHash() + "', "
                        + blk.getTime() + ","
                        + blk.getBlockSize() + ");";
                stmt.executeUpdate(sql);
            }

            stmt.close();
            conn.commit();
            conn.close();
        } catch (Exception e) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        //System.out.println("BLK: Records created successfully");
    }

    public static void insertTransactions(List<Tx> txs, Connection conn, Statement stmt) {
        // Txs
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/blockchain",
                            "postgres", "jalokim123");
            conn.setAutoCommit(false);
            //System.out.println("TX: Database opened successfully");

            // Insert records
            for (Tx tx : txs) {
                stmt = conn.createStatement();
                String sql = "INSERT INTO tx (id, hash, coinbase, out_value, tx_size, blk_id) VALUES ("
                        + tx.getId() + ", "
                        + "'" + tx.getTransactionHash() + "', "
                        + tx.isCoinbase() + ","
                        + tx.getOutValue() + ","
                        + tx.getTransactionSize() + ", "
                        + tx.getBlk_id() + ");";
                stmt.executeUpdate(sql);
            }

            stmt.close();
            conn.commit();
            conn.close();
        } catch (Exception e) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        //System.out.println("TX: Records created successfully");
    }

    public static void insertTransactionInputs(List<Txin> txins, Connection conn, Statement stmt) {
        // Txins
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/blockchain",
                            "postgres", "jalokim123");
            conn.setAutoCommit(false);
            //System.out.println("TXIN: Database opened successfully");

            // Insert records
            for (Txin txin : txins) {
                stmt = conn.createStatement();
                String sql = "INSERT INTO txin (id, tx_idx, prev_out, prev_out_index, tx_id) VALUES ("
                        + txin.getId() + ", "
                        + txin.getTxinIndex() + ","
                        + "'" + txin.getPrevTransactionHash() + "', "
                        + txin.getPrevTransactionIndex() + ","
                        + txin.getTx_id() + ");";
                stmt.executeUpdate(sql);
            }

            stmt.close();
            conn.commit();
            conn.close();
        } catch (Exception e) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        //System.out.println("TXIN: Records created successfully");
    }

    public static void insertTransactionOutputs(List<Txout> txouts, Connection conn, Statement stmt) {
        // Txouts
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/blockchain",
                            "postgres", "jalokim123");
            conn.setAutoCommit(false);
            //System.out.println("TXOUT: Database opened successfully");

            // Insert records
            for (Txout txout : txouts) {
                stmt = conn.createStatement();
                String sql = "INSERT INTO txout (id, tx_idx, address, value, type, tx_id) VALUES ("
                        + txout.getId() + ", "
                        + txout.getTxoutIndex() + ","
                        + "'" + txout.getOutAddress() + "', "
                        + txout.getValue() + ","
                        + "'" + txout.getTypeStr() + "', "
                        + txout.getTx_id() + ");";
                stmt.executeUpdate(sql);
            }

            stmt.close();
            conn.commit();
            conn.close();
        } catch (Exception e) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        //System.out.println("TXOUT: Records created successfully");
    }

    public static void insertAll(Parsed parsed, Connection conn, Statement stmt) {
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/blockchain",
                            "postgres", "jalokim123");
            conn.setAutoCommit(false);
            //System.out.println("BLK: Database opened successfully");

            // Insert blk
            for (Blk blk : parsed.getBlks()) {
                stmt = conn.createStatement();
                String sql = "INSERT INTO blk (id, hash, prev_hash, time, blk_size) VALUES ("
                        + blk.getId() + ", "
                        + "'" + blk.getBlockHash() + "', "
                        + "'" + blk.getPrevBlockHash() + "', "
                        + blk.getTime() + ","
                        + blk.getBlockSize() + ");";
                stmt.executeUpdate(sql);
            }

            // Insert tx
            for (Tx tx : parsed.getTxs()) {
                stmt = conn.createStatement();
                String sql = "INSERT INTO tx (id, hash, coinbase, out_value, tx_size, blk_id) VALUES ("
                        + tx.getId() + ", "
                        + "'" + tx.getTransactionHash() + "', "
                        + tx.isCoinbase() + ","
                        + tx.getOutValue() + ","
                        + tx.getTransactionSize() + ", "
                        + tx.getBlk_id() + ");";
                stmt.executeUpdate(sql);
            }

            // Insert txin
            for (Txin txin : parsed.getTxins()) {
                stmt = conn.createStatement();
                String sql = "INSERT INTO txin (id, tx_idx, prev_out, prev_out_index, tx_id) VALUES ("
                        + txin.getId() + ", "
                        + txin.getTxinIndex() + ","
                        + "'" + txin.getPrevTransactionHash() + "', "
                        + txin.getPrevTransactionIndex() + ","
                        + txin.getTx_id() + ");";
                stmt.executeUpdate(sql);
            }

            // Insert txout
            for (Txout txout : parsed.getTxouts()) {
                stmt = conn.createStatement();
                String sql = "INSERT INTO txout (id, tx_idx, address, value, type, tx_id) VALUES ("
                        + txout.getId() + ", "
                        + txout.getTxoutIndex() + ","
                        + "'" + txout.getOutAddress() + "', "
                        + txout.getValue() + ","
                        + "'" + txout.getTypeStr() + "', "
                        + txout.getTx_id() + ");";
                stmt.executeUpdate(sql);
            }

            stmt.close();
            conn.commit();
            conn.close();
        } catch (Exception e) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
    }

    public static void createHdfsFiles(Parsed parsed, String path, long id, Configuration conf) throws IOException {

        FileSystem fs = FileSystem.newInstance(conf);
        Path blkHdfsPath = new Path(path + "blk" + String.valueOf(id));
        Path txHdfsPath = new Path(path + "tx" + String.valueOf(id));
        Path txinHdfsPath = new Path(path + "intx" + String.valueOf(id));
        Path txoutHdfsPath = new Path(path + "outtx" + String.valueOf(id));
        //fShell.setrepr((short) 1, filePath);
        fs.createNewFile(blkHdfsPath);
        fs.createNewFile(txHdfsPath);
        fs.createNewFile(txinHdfsPath);
        fs.createNewFile(txoutHdfsPath);

        // Create blk
        String blkRecords = "";
        FSDataOutputStream blkOutputStream = null;
        for (Blk blk : parsed.getBlks()) {
            String blkRecord = String.valueOf(blk.getId()) + ","
                    + blk.getBlockHash() + ","
                    + blk.getPrevBlockHash() + ","
                    + String.valueOf(blk.getTime()) + ","
                    + String.valueOf(blk.getBlockSize()) + "\n";
            blkRecords = blkRecords + blkRecord;
        }
        blkOutputStream = fs.append(blkHdfsPath);
        //blkOutputStream.writeChars(blkRecords);
        blkOutputStream.writeBytes(blkRecords); // try
        if (blkOutputStream != null) {
            blkOutputStream.close();
        }

        // Create tx
        String txRecords = "";
        FSDataOutputStream txOutputStream = null;
        for (Tx tx : parsed.getTxs()) {
            String txRecord = String.valueOf(tx.getId()) + ","
                    + tx.getTransactionHash() + ","
                    + String.valueOf(tx.isCoinbase()) + ","
                    + String.valueOf(tx.getOutValue()) + ","
                    + String.valueOf(tx.getTransactionSize()) + ","
                    + String.valueOf(tx.getBlk_id()) + "\n";
            txRecords = txRecords + txRecord;
        }
        txOutputStream = fs.append(txHdfsPath);
        //txOutputStream.writeChars(txRecords);
        txOutputStream.writeBytes(txRecords); // try
        if (txOutputStream != null) {
            txOutputStream.close();
        }

        // Create txin
        String txinRecords = "";
        FSDataOutputStream txinOutputStream = null;
        for (Txin txin : parsed.getTxins()) {
            String txinRecord = String.valueOf(txin.getId()) + ","
                    + String.valueOf(txin.getTxinIndex()) + ","
                    + txin.getPrevTransactionHash() + ","
                    + String.valueOf(txin.getPrevTransactionIndex()) + ","
                    + String.valueOf(txin.getTx_id()) + "\n";
            txinRecords = txinRecords + txinRecord;
        }
        txinOutputStream = fs.append(txinHdfsPath);
        //txinOutputStream.writeChars(txinRecords);
        txinOutputStream.writeBytes(txinRecords); // try
        if (txinOutputStream != null) {
            txinOutputStream.close();
        }

        // Create txout
        String txoutRecords = "";
        FSDataOutputStream txoutOutputStream = null;
        for (Txout txout : parsed.getTxouts()) {
            String txoutRecord = String.valueOf(txout.getId()) + ","
                    + String.valueOf(txout.getTxoutIndex()) + ","
                    + txout.getOutAddress() + ","
                    + String.valueOf(txout.getValue()) + ","
                    + txout.getTypeStr() + ","
                    + String.valueOf(txout.getTx_id()) + "\n";
            txoutRecords = txoutRecords + txoutRecord;
        }
        txoutOutputStream = fs.append(txoutHdfsPath);
        //txoutOutputStream.writeChars(txoutRecords);
        txoutOutputStream.writeBytes(txoutRecords); // try
        if (txoutOutputStream != null) {
            txoutOutputStream.close();
        }

        // close FileSystem and FSDataOutputStream
        if (fs != null) {
            fs.close();
        }
    }

    public static void createHdfsFilesTask(Parsed parsed, String path, int taskId, Configuration conf) throws IOException {

        FileSystem fs = FileSystem.newInstance(conf);
        Path blkHdfsPath = new Path(path + "blk" + String.valueOf(taskId));
        Path txHdfsPath = new Path(path + "tx" + String.valueOf(taskId));
        Path txinHdfsPath = new Path(path + "intx" + String.valueOf(taskId));
        Path txoutHdfsPath = new Path(path + "outtx" + String.valueOf(taskId));

        // Create blk
        String blkRecords = "";
        FSDataOutputStream blkOutputStream = null;
        for (Blk blk : parsed.getBlks()) {
            String blkRecord = String.valueOf(blk.getId()) + ";"
                    + blk.getBlockHash() + ";"
                    + blk.getPrevBlockHash()
                    + String.valueOf(blk.getTime()) + ";"
                    + String.valueOf(blk.getBlockSize()) + "\n";
            blkRecords = blkRecords + blkRecord;
        }
        blkOutputStream = fs.append(blkHdfsPath);
        //blkOutputStream.writeChars(blkRecords);
        blkOutputStream.writeBytes(blkRecords);
        if (blkOutputStream != null) {
            blkOutputStream.close();
        }

        // Create tx
        String txRecords = "";
        FSDataOutputStream txOutputStream = null;
        for (Tx tx : parsed.getTxs()) {
            String txRecord = String.valueOf(tx.getId()) + ";"
                    + tx.getTransactionHash() + ";"
                    + String.valueOf(tx.isCoinbase()) + ";"
                    + String.valueOf(tx.getOutValue()) + ";"
                    + String.valueOf(tx.getTransactionSize()) + ";"
                    + String.valueOf(tx.getBlk_id()) + "\n";
            txRecords = txRecords + txRecord;
        }
        txOutputStream = fs.append(txHdfsPath);
        //txOutputStream.writeChars(txRecords);
        txOutputStream.writeBytes(txRecords);
        if (txOutputStream != null) {
            txOutputStream.close();
        }

        // Create txin
        String txinRecords = "";
        FSDataOutputStream txinOutputStream = null;
        for (Txin txin : parsed.getTxins()) {
            String txinRecord = String.valueOf(txin.getId()) + ";"
                    + String.valueOf(txin.getTxinIndex()) + ";"
                    + txin.getPrevTransactionHash() + ";"
                    + String.valueOf(txin.getPrevTransactionIndex()) + ";"
                    + String.valueOf(txin.getTx_id()) + "\n";
            txinRecords = txinRecords + txinRecord;
        }
        txinOutputStream = fs.append(txinHdfsPath);
        //txinOutputStream.writeChars(txinRecords);
        txinOutputStream.writeBytes(txinRecords);
        if (txinOutputStream != null) {
            txinOutputStream.close();
        }

        // Create txout
        String txoutRecords = "";
        FSDataOutputStream txoutOutputStream = null;
        for (Txout txout : parsed.getTxouts()) {
            String txoutRecord = String.valueOf(txout.getId()) + ";"
                    + String.valueOf(txout.getTxoutIndex()) + ";"
                    + txout.getOutAddress() + ";"
                    + String.valueOf(txout.getValue()) + ";"
                    + txout.getTypeStr() + ";"
                    + String.valueOf(txout.getTx_id()) + "\n";
            txoutRecords = txoutRecords + txoutRecord;
        }
        txoutOutputStream = fs.append(txoutHdfsPath);
        //txoutOutputStream.writeChars(txoutRecords);
        txoutOutputStream.writeBytes(txoutRecords);
        if (txoutOutputStream != null) {
            txoutOutputStream.close();
        }

        // close FileSystem and FSDataOutputStream
        if (fs != null) {
            fs.close();
        }
    }

    public static List<byte[]> splitBlocks(byte[] array, byte[] delimiter) {
        // Splits binary data from blk.dat files into list of binary blocks
        // From: https://stackoverflow.com/questions/22519346/how-to-split-a-byte-array-around-a-byte-sequence-in-java
        List<byte[]> byteArrays = new LinkedList<byte[]>();
        if (delimiter.length == 0) {
            return byteArrays;
        }
        int begin = 0;

        outer: for (int i = 0; i < array.length - delimiter.length + 1; i++) {
            for (int j = 0; j < delimiter.length; j++) {
                if (array[i + j] != delimiter[j]) {
                    continue outer;
                }
            }
            // If delimiter is at the beginning then there will not be any data.
            if (begin != i)
                byteArrays.add(Arrays.copyOfRange(array, begin + 4, i));
            begin = i + delimiter.length;
        }
        // delimiter at the very end with no data following?
        if (begin != array.length)
            byteArrays.add(Arrays.copyOfRange(array, begin + 4, array.length));

        return byteArrays;
    }

    public static int countInputHdfsFiles(Configuration conf, String path) throws IOException {
        // Call with args[args.length - 2] or with args[0] - should be the same
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] listedHdfsFilesStatuses = fs.listStatus(new Path(path)); // input agrs

        // For debugging
        //System.out.println("Number of HDFS files statuses: " + listedHdfsFilesStatuses.length);
        //for (FileStatus fileStatus : listedHdfsFilesStatuses) {
        //    System.out.println(fileStatus.toString());
        //}
        return listedHdfsFilesStatuses.length;
    }

    public static void generateReport(String programName, String execDate, int filesCount, long totalTime) {
        try{
            PrintWriter writer = new PrintWriter(programName + "_" + execDate + ".txt", "UTF-8");
            writer.println(programName);
            writer.println("Execution date: " + execDate);
            writer.println("Number of blk files: " + String.valueOf(filesCount)); // input args
            writer.println("Execution time in ms: " + String.valueOf(totalTime));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}