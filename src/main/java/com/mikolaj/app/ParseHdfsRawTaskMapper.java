package com.mikolaj.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.bitcoinj.core.Block;
import org.bitcoinj.params.MainNetParams;

import java.io.IOException;
import java.util.Arrays;

/**
 * Parses block, transactions, transaction inputs and transaction outputs to HDFS files - 1 file per task, per type
 */
public class ParseHdfsRawTaskMapper extends Mapper<BytesWritable, BytesWritable, NullWritable, Text> {

    private static Logger logger = Logger.getLogger(ParseHdfsRawTaskMapper.class.getName());
    private final org.bitcoinj.core.Context bitcoinContext = new org.bitcoinj.core.Context(MainNetParams.get());

    // uid solution, idea from: http://shzhangji.com/blog/2013/10/31/generate-auto-increment-id-in-map-reduce-job/
    private long id;
    private int increment;
    private int taskId;

    private Text outKey = new Text();
    private Text outValue = new Text();
    private String delim = ";"; // separator

    private MultipleOutputs<NullWritable, Text> output;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        taskId = context.getTaskAttemptID().getTaskID().getId();
        id = context.getTaskAttemptID().getTaskID().getId();
        increment = context.getConfiguration().getInt("mapred.map.tasks", 0);
        if (increment == 0) {
            throw new IllegalArgumentException("mapred.map.tasks is zero");
        }

        output = new MultipleOutputs(context);
    }

    @Override
    public void map(BytesWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        // parse blockchain file and emit to reducer per type per taskId
        Configuration conf = context.getConfiguration();
        //Text outKey = new Text();
        //Text outValue = new Text();

        // uid solution
        id += increment;

        // omit first 8 bytes: magic bytes (4) and block size (4)
        byte[] valueByte = Arrays.copyOfRange(value.getBytes(), 8, value.getBytes().length);
        Block block = new Block(MainNetParams.get(), valueByte);

        logger.info("TaskAttempt " + context.getTaskAttemptID().getTaskID().getId() + " with assigned UID: " + String.valueOf(id));

        //System.out.println("\nParsing " + file.toString() + " with GID starting from " + String.valueOf(i * gid));
        Parsed parsed = MyUtils.parse(block, id);

        outKey.set("B" + String.valueOf(taskId)); // key B for blk
        for (Blk blk : parsed.getBlks()) {
            outValue.set(String.valueOf(blk.getId()) + delim
                    + blk.getPrevBlockHash() + delim
                    + blk.getBlockHash() + delim
                    + String.valueOf(blk.getTime()) + delim
                    + String.valueOf(blk.getBlockSize()));
            //context.write(outKey, outValue);
            output.write(NullWritable.get(), outValue, generateFileName(outKey));
        }

        outKey.set("T" + String.valueOf(taskId)); // key T for tx
        for (Tx tx : parsed.getTxs()) {
            outValue.set(String.valueOf(tx.getId()) + delim
                    + tx.getTransactionHash() + delim
                    + String.valueOf(tx.isCoinbase()) + delim
                    + String.valueOf(tx.getOutValue()) + delim
                    + String.valueOf(tx.getTransactionSize()) + delim
                    + String.valueOf(tx.getBlk_id()));
            //context.write(outKey, outValue);
            output.write(NullWritable.get(), outValue, generateFileName(outKey));
        }

        outKey.set("I" + String.valueOf(taskId)); // key I for txin
        for (Txin txin : parsed.getTxins()) {
            outValue.set(String.valueOf(txin.getId()) + delim
                    + String.valueOf(txin.getTxinIndex()) + delim
                    + txin.getPrevTransactionHash() + delim
                    + String.valueOf(txin.getPrevTransactionIndex()) + delim
                    + String.valueOf(txin.getTx_id()));
            //context.write(outKey, outValue);
            output.write(NullWritable.get(), outValue, generateFileName(outKey));
        }

        outKey.set("O" + String.valueOf(taskId)); // key O for txout
        for (Txout txout : parsed.getTxouts()) {
            outValue.set(String.valueOf(txout.getId()) + delim
                    + String.valueOf(txout.getTxoutIndex()) + delim
                    + txout.getOutAddress() + delim
                    + String.valueOf(txout.getValue()) + delim
                    + txout.getTypeStr() + delim
                    + String.valueOf(txout.getTx_id()));
            //context.write(outKey, outValue);
            output.write(NullWritable.get(), outValue, generateFileName(outKey));
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
        output.close();
    }

    private String generateFileName(Text k) {
        if (k.charAt(0) == 'B') {
            return "blk" + k.toString().substring(1);
        } else if (k.charAt(0) == 'T') {
            return "tx" + k.toString().substring(1);
        } else if (k.charAt(0) == 'I') {
            return "txin" + k.toString().substring(1);
        } else if (k.charAt(0) == 'O') {
            return "txout" + k.toString().substring(1);
        } else {
            return "error";
        }
    }
}

