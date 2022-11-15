package com.study.frends.job02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

public class FriendMysqlReducer extends Reducer<Text, IntWritable, Friend, NullWritable> {

    private String jobName;

    // 在执行reduce方法之前执行
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        jobName = context.getJobName();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        // 初始化 关系值和 与 关系次数和
        int sum = 0, count = 0;
        // 循环处理
        for (IntWritable value : values) {
            // 关系值求和
            sum += value.get();
            // 统计关系次数
            count += 1;
        }
        // 当关系值和与关系次数和相等时才需要进行推荐
        if (sum == count) {
            String[] loginUserAndFields = key.toString().split(":");
            String[] loginUser = loginUserAndFields[0].split("_");
            String[] friend = loginUserAndFields[1].split("_");
            Friend f1 = new Friend(jobName + "-" + UUID.randomUUID().toString().replaceAll("-", ""),
                    Integer.parseInt(loginUser[0]), loginUser[1], Integer.parseInt(friend[0]), friend[1], sum, new Date(), new Date());
            Friend f2 = new Friend(jobName + "-" + UUID.randomUUID().toString().replaceAll("-", ""),
                    Integer.parseInt(friend[0]), friend[1], Integer.parseInt(loginUser[0]), loginUser[1], sum, new Date(), new Date());
            // 写出数据
            context.write(f1, NullWritable.get());
            context.write(f2, NullWritable.get());
        }
    }

}
