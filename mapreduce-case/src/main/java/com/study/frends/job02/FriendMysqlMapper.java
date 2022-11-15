package com.study.frends.job02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendMysqlMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String loginUser = value.toString().split(":")[0];
        String[] friends = value.toString().split(":")[1].split("\\s");
        for (int i = 0; i < friends.length; i++) {
            context.write(new Text(sortName(loginUser, friends[i])), new IntWritable(0));
            // 计算间接好友，关系值为 1
            for (int j = i + 1; j < friends.length; j++) {
                context.write(new Text(sortName(friends[i], friends[j])), new IntWritable(1));
            }
        }
    }

    private String sortName(String f1, String f2) {
        return f1.compareTo(f2) < 1 ? f1 + ":" + f2 : f2 + ":" + f1;
    }

}
