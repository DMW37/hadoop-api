package com.study.wzry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: 邓明维
 * @date: 2022/11/15
 * @description:
 */

/**
 * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN:当前行偏移量
 *     VALUEIN:当前行数据
 *     KEYOUT:输出数据的Key
 *     VALUEOUT:输出数据的Value
 */
public class GOFMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private IntWritable one = new IntWritable(1);
    /**
     *
     * @param key       前行的偏移量
     * @param value     当前行的数据
     * @param context   可以理解为环形数据缓冲区
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 忽略第一行数据
        if (key.get()==0){
            return;
        }
        // 平台,大区,用户名,充值时间,性别,充值金额
        // QQ,5,Cf17ZZFZXm,1610268278116,man,18
        String[] r = value.toString().split(",");
        if (r.length==6){
            // 性别作为key 充值金额作为value
            // 注意：此处的man和woman分区后在同一个区,如果想要随机分区，可以使用随机的方式
            context.write(new Text(r[4]),new IntWritable(Integer.valueOf(r[5])));
        }

    }
}
