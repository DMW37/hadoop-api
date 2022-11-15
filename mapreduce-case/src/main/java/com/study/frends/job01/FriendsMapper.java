package com.study.frends.job01;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     *  处理map任务的方法
     * @param key 偏移量
     * @param value 一行数据
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

       // 23_何玉洁:17_郭燕芳 35_蔡雯蔚 33_黄龙剑 55_李鸿涛
        String loginUser = value.toString().split(":")[0];
        String[] friends = value.toString().split(":")[1].split("\\s+");
        // 遍历friends
        for (int i = 0; i < friends.length; i++){
            // loginUser和friends[i] 组合都是直接好友，  亲密度都为0
            // 写出数据
            context.write(new Text(mergeString(loginUser, friends[i])), new IntWritable(0));
            for (int j = i + 1; j < friends.length; j ++){
                // 这里面都是间接好友的情况， 亲密度都为  1
                context.write(new Text(mergeString(friends[i], friends[j])), new IntWritable(1));
            }
        }
    }

    private String mergeString(String s1, String s2){

        return s1.compareTo(s2) > 0? s1+":"+s2 : s2+":"+s1;
    }
}
