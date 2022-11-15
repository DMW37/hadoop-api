package com.study.frends.job01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendsReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     *
     * @param key map写出的数据
     * @param values map写出的value的集合
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 定义两个变量， 一个用来表示好友出现的次数， 一个用来表示亲密度。。。。  只有当亲密度 == 好友次数的时候， 系统才会推荐。  我们才会写出对应的结果
        int count = 0;
        int intimacy = 0;

        for (IntWritable value : values) {
            intimacy += value.get();
            count++;
        }

        if (count == intimacy){
            // 全部都是间接好友的情况
            context.write(key, new IntWritable(count));
        }
    }
}
