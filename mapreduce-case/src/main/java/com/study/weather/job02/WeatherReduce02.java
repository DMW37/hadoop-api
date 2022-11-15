package com.study.weather.job02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

public class WeatherReduce02 extends Reducer<Weather, IntWritable, Text, NullWritable> {

    /**
     *
     * @param key map写出的数据
     * @param values map写出的value的集合
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Weather key, Iterable<IntWritable> values, Reducer<Weather, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        // reduce 方法执行一次就是处理一组数据 ，   而一组数据就是  省地区年月日  相同的数据
        Set<String> weatherSet = new HashSet<>();
        // 最终写出的数据应该是   ：   广东省南沙区2020年6月1日   广东省南沙区2020年6月3日   广东省南沙区2020年6月7日
        for (IntWritable temp : values) {
            StringBuffer sb = new StringBuffer();
            sb.append(key.getProvince()).append(key.getCity())
                    .append(DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.parse(key.getReportTime(), DateTimeFormatter.ofPattern("d/M/yyyy HH:mm:ss"))));

            // 往set中存储   省份地区年月日    广东省南沙区20200610   38    分组的条件  省地区年月
            //  广东省南沙区20200610   39
            //  广东省南沙区20200610   38
            //  广东省南沙区20200610   38
            //  广东省南沙区20200610   37

            // 将 sb 存入set集合
            if (!weatherSet.contains(sb.toString())){
                weatherSet.add(sb.toString());
                context.write(new Text(sb.append("【").append(key.getTemperature()).append("】").toString()), NullWritable.get());
            }

            if (weatherSet.size() == 3){
                return;
            }

        }
    }
}
