package com.mikolaj.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Context;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.MainNetParams;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;

/**
 * Created by Mikolaj on 03.08.17.
 *
 * TODO: throws exception
 *
 */
public class ParseHdfsRawTaskMapper extends Mapper<BytesWritable, BytesWritable, NullWritable, NullWritable> {

    private static Logger logger = Logger.getLogger(ParseHdfsRawMapper.class.getName());
    private final org.bitcoinj.core.Context bitcoinContext = new org.bitcoinj.core.Context(MainNetParams.get());
    private Connection conn = null;
    private Statement stmt = null;

    // uid solution, idea from: http://shzhangji.com/blog/2013/10/31/generate-auto-increment-id-in-map-reduce-job/
    private long id;
    private int increment;
    private int taskId;
    //private final int gid = 1000000;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        taskId = context.getTaskAttemptID().getTaskID().getId();
        id = context.getTaskAttemptID().getTaskID().getId();
        increment = context.getConfiguration().getInt("mapred.map.tasks", 0);
        if (increment == 0) {
            throw new IllegalArgumentException("mapred.map.tasks is zero");
        }
        // Create hdfs file
        FileSystem fs = FileSystem.newInstance(context.getConfiguration());
        Path blkHdfsPath = new Path("/user/Mikolaj/blockchain/output/blk" + String.valueOf(taskId));
        Path txHdfsPath = new Path("/user/Mikolaj/blockchain/output/tx" + String.valueOf(taskId));
        Path txinHdfsPath = new Path("/user/Mikolaj/blockchain/output/intx" + String.valueOf(taskId));
        Path txoutHdfsPath = new Path("/user/Mikolaj/blockchain/output/outtx" + String.valueOf(taskId));
        //fShell.setrepr((short) 1, filePath);
        fs.createNewFile(blkHdfsPath);
        fs.createNewFile(txHdfsPath);
        fs.createNewFile(txinHdfsPath);
        fs.createNewFile(txoutHdfsPath);
        fs.close();
    }

    @Override
    public void map(BytesWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        // parse blockchain file and append to HDFS output files
        Configuration conf = context.getConfiguration();

        // uid solution
        id += increment;

        // omit first 8 bytes: magic bytes (4) and block size (4)
        byte[] valueByte = Arrays.copyOfRange(value.getBytes(), 8, value.getBytes().length);
        Block block = new Block(MainNetParams.get(), valueByte);

        logger.info("TaskAttempt " + context.getTaskAttemptID().getTaskID().getId() + " with assigned UID: " + String.valueOf(id));

        //System.out.println("\nParsing " + file.toString() + " with GID starting from " + String.valueOf(i * gid));
        Parsed parsed = MyUtils.parse(block, id);

        // dump parsed data to HDFS files, one file per Task
        MyUtils.createHdfsFilesTask(parsed, "/user/Mikolaj/blockchain/output/", taskId, conf);

        // emit nothing
        context.write(NullWritable.get(), NullWritable.get());
    }
}
