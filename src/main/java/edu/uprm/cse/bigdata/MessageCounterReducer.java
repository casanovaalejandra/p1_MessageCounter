package edu.uprm.cse.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class MessageCounterReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        // setup a counter
        int count = 0;
        // iterator over list of 1s, to count them (no size() or length() method available)
        for (IntWritable value : values ){
            count +=value.get();

        }
        System.out.println(count+" "+key);

        // DEBUG
        context.write(key, new IntWritable(count));
    }
}
