package com.study.weather.job02;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 一个对象封装一条记录
 */
public class Weather implements WritableComparable<Weather> {
    private String id;
    private String province;
    private String city;
    private String areaCode;
    private String wea;
    private String temperature;
    private String windDirection;
    private String windPower;
    private String humidity;
    private String reportTime;
    private String createTime;

    @Override
    public int compareTo(Weather wea) {
        // 定义排序规则  ，
        // 根据areaCode排序
        int result = this.areaCode.compareTo(wea.getAreaCode());
        if(result != 0){
            return result;
        }
        // 根据年月日排序
        result = this.reportTime.split(" ")[0].substring(this.reportTime.split(" ")[0].indexOf("/"))
                .compareTo(wea.getReportTime().split(" ")[0].substring(wea.getReportTime().split(" ")[0].indexOf("/")));
        if(result != 0){
            return result;
        }
        // 根据温度排序
        return result = this.temperature.compareTo(wea.getTemperature());
    }


    /**
     * 注意  写的顺序和读的顺序要保持一致
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.getId());
        out.writeUTF(this.getProvince());
        out.writeUTF(this.getCity());
        out.writeUTF(this.getAreaCode());
        out.writeUTF(this.getWea());
        out.writeUTF(this.getTemperature());
        out.writeUTF(this.getWindDirection());
        out.writeUTF(this.getWindPower());
        out.writeUTF(this.getHumidity());
        out.writeUTF(this.getReportTime());
        out.writeUTF(this.getCreateTime());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.setId(in.readUTF());
        this.setProvince(in.readUTF());
        this.setCity(in.readUTF());
        this.setAreaCode(in.readUTF());
        this.setWea(in.readUTF());
        this.setTemperature(in.readUTF());
        this.setWindDirection(in.readUTF());
        this.setWindPower(in.readUTF());
        this.setHumidity(in.readUTF());
        this.setReportTime(in.readUTF());
        this.setCreateTime(in.readUTF());
    }


    public Weather() {
    }

    public Weather(String id, String province, String city, String areaCode, String wea, String temperature, String windDirection, String windPower, String humidity, String reportTime, String createTime) {
        this.id = id;
        this.province = province;
        this.city = city;
        this.areaCode = areaCode;
        this.wea = wea;
        this.temperature = temperature;
        this.windDirection = windDirection;
        this.windPower = windPower;
        this.humidity = humidity;
        this.reportTime = reportTime;
        this.createTime = createTime;
    }

    /**
     * 获取
     * @return id
     */
    public String getId() {
        return id;
    }

    /**
     * 设置
     * @param id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * 获取
     * @return province
     */
    public String getProvince() {
        return province;
    }

    /**
     * 设置
     * @param province
     */
    public void setProvince(String province) {
        this.province = province;
    }

    /**
     * 获取
     * @return city
     */
    public String getCity() {
        return city;
    }

    /**
     * 设置
     * @param city
     */
    public void setCity(String city) {
        this.city = city;
    }

    /**
     * 获取
     * @return areaCode
     */
    public String getAreaCode() {
        return areaCode;
    }

    /**
     * 设置
     * @param areaCode
     */
    public void setAreaCode(String areaCode) {
        this.areaCode = areaCode;
    }

    /**
     * 获取
     * @return wea
     */
    public String getWea() {
        return wea;
    }

    /**
     * 设置
     * @param wea
     */
    public void setWea(String wea) {
        this.wea = wea;
    }

    /**
     * 获取
     * @return temperature
     */
    public String getTemperature() {
        return temperature;
    }

    /**
     * 设置
     * @param temperature
     */
    public void setTemperature(String temperature) {
        this.temperature = temperature;
    }

    /**
     * 获取
     * @return windDirection
     */
    public String getWindDirection() {
        return windDirection;
    }

    /**
     * 设置
     * @param windDirection
     */
    public void setWindDirection(String windDirection) {
        this.windDirection = windDirection;
    }

    /**
     * 获取
     * @return windPower
     */
    public String getWindPower() {
        return windPower;
    }

    /**
     * 设置
     * @param windPower
     */
    public void setWindPower(String windPower) {
        this.windPower = windPower;
    }

    /**
     * 获取
     * @return humidity
     */
    public String getHumidity() {
        return humidity;
    }

    /**
     * 设置
     * @param humidity
     */
    public void setHumidity(String humidity) {
        this.humidity = humidity;
    }

    /**
     * 获取
     * @return reportTime
     */
    public String getReportTime() {
        return reportTime;
    }

    /**
     * 设置
     * @param reportTime
     */
    public void setReportTime(String reportTime) {
        this.reportTime = reportTime;
    }

    /**
     * 获取
     * @return createTime
     */
    public String getCreateTime() {
        return createTime;
    }

    /**
     * 设置
     * @param createTime
     */
    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "Weather{id = " + id + ", province = " + province + ", city = " + city + ", areaCode = " + areaCode + ", wea = " + wea + ", temperature = " + temperature + ", windDirection = " + windDirection + ", windPower = " + windPower + ", humidity = " + humidity + ", reportTime = " + reportTime + ", createTime = " + createTime + "}";
    }

}
