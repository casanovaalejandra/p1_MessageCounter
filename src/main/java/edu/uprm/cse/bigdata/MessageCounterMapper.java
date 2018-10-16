package edu.uprm.cse.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.omg.PortableInterceptor.INACTIVE;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

public class MessageCounterMapper extends Mapper<LongWritable,Text,Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        //The purpose of this class is to count all the messages that a user has done
        String tweet = value.toString();
        Status status = null;
        Text user_id = null;
        try {
            status = TwitterObjectFactory.createStatus(tweet);
            user_id = new Text (String.valueOf(status.getUser().getId()));
            context.write(user_id, new IntWritable(1));

        } catch (TwitterException e) {
            e.printStackTrace();
        }
    }
}
