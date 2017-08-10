package com.mikolaj.app;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Mikolaj on 13.07.17.
 */
public class TopTxReducer extends Reducer<NullWritable, MapWritable, Text, LongWritable> {

    // Stores a map of outAddresses to value of Satoshis
    private TreeMap<Writable, Writable> outAddressSatoshiMap = new TreeMap<Writable, Writable>();

    @Override
    public void reduce(NullWritable nullKey, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {

        for (MapWritable value : values) {
            for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
                // keeping top 10
                outAddressSatoshiMap.put(entry.getKey(), entry.getValue());
                if (outAddressSatoshiMap.size() > 10) {
                    outAddressSatoshiMap.remove(outAddressSatoshiMap.firstKey());
                }
            }
        }

        // emitting
        for (Map.Entry<Writable, Writable> element : outAddressSatoshiMap.entrySet()) {
            Text outAddress = new Text(element.getValue().toString());
            LongWritable satoshi = new LongWritable(Long.valueOf(element.getKey().toString()));
            context.write(outAddress, satoshi);
        }

    }

}
