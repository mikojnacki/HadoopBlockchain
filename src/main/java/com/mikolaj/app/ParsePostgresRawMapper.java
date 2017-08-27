package com.mikolaj.app;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.log4j.Logger;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Context;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.MainNetParams;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Mikolaj on 03.08.17.
 *
 * TODO: UID solution
 *
 */
public class ParsePostgresRawMapper extends Mapper<BytesWritable, BytesWritable, NullWritable, NullWritable> {

    private static Logger logger = Logger.getLogger(ParsePostgresRawMapper.class.getName());
    private final org.bitcoinj.core.Context bitcoinContext = new org.bitcoinj.core.Context(MainNetParams.get());
    private Connection conn = null;
    private Statement stmt = null;

    // uid solution, idea from: http://shzhangji.com/blog/2013/10/31/generate-auto-increment-id-in-map-reduce-job/
    private long id;
    private int increment;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        id = context.getTaskAttemptID().getTaskID().getId();
        //increment = context.getConfiguration().getInt("mapred.map.tasks", 0);
        increment = context.getConfiguration().getInt("mapreduce.job.maps", 0);
        if (increment == 0) {
            throw new IllegalArgumentException("mapred.map.tasks is zero");
        }
    }


    @Override
    public void map(BytesWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        // parse blockchain file and insert it into PostgreSQL db

        // uid solution
        id += increment;

        // omit first 8 bytes: magic bytes (4) and block size (4)
        byte[] valueByte = Arrays.copyOfRange(value.getBytes(), 8, value.getBytes().length);
        Block block = new Block(MainNetParams.get(), valueByte);

        logger.info("TaskAttempt " + context.getTaskAttemptID().getTaskID().getId() + " with assigned UID: " + String.valueOf(id));

        //System.out.println("\nParsing " + file.toString() + " with GID starting from " + String.valueOf(i * gid));
        Parsed parsed = MyUtils.parse(block, id);

        // insert into PostgreSQL db
        MyUtils.insertAll(parsed, conn, stmt);
        //MyUtils.insertBlocks(parsed.getBlks(), conn, stmt);
        //MyUtils.insertTransactions(parsed.getTxs(), conn, stmt);
        //MyUtils.insertTransactionInputs(parsed.getTxins(), conn, stmt);
        //MyUtils.insertTransactionOutputs(parsed.getTxouts(), conn, stmt);

        // emit nothing
        context.write(NullWritable.get(), NullWritable.get());
    }
}
