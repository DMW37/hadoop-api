package com.study.weather.job02;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author woaixiejuan
 */
public class WeatherGroupingComparator extends WritableComparator {


    // 需要使用启动该分组器
    WeatherGroupingComparator(){
        super(Weather.class,true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Weather wea1 = (Weather) a;
        Weather wea2 = (Weather) b;

        int result = wea1.getAreaCode().compareTo(wea2.getAreaCode());
        if(result != 0){
            return result;
        }
        // 根据年月日排序
        return wea1.getReportTime().split(" ")[0].substring(wea1.getReportTime().split(" ")[0].indexOf("/"))
                .compareTo(wea2.getReportTime().split(" ")[0].substring(wea2.getReportTime().split(" ")[0].indexOf("/")));

    }
}
