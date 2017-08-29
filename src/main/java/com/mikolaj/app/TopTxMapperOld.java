package com.mikolaj.app;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.record.Record;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Mikolaj on 13.07.17.
 */
public class TopTxMapperOld extends Mapper<Object, Text, NullWritable, MapWritable> {

    // Stores a map of outAddresses to value of Satoshis
    private TreeMap<LongWritable, Text> outAddressSatoshiMap = new TreeMap<LongWritable, Text>();

    @Override
    public void map(Object key, Text input, Context context) throws IOException, InterruptedException {

        String[] inputString = input.toString().split("\\t");
        Text outAddress = new Text(inputString[0]);
        LongWritable satoshi = new LongWritable(Long.valueOf(inputString[1]));
        // sorted on values (becomes TreeMap keys) in ascending order
        outAddressSatoshiMap.put(satoshi, outAddress);
        if (outAddressSatoshiMap.size() > 10) {
            outAddressSatoshiMap.remove(outAddressSatoshiMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (Map.Entry<LongWritable, Text> element : outAddressSatoshiMap.entrySet()) {
            System.out.println(element.getValue().toString() + "\t" + element.getKey().toString());
            MapWritable output = new MapWritable();
            output.put(element.getKey(), element.getValue());
            context.write(NullWritable.get(), output);
        }
    }
}
