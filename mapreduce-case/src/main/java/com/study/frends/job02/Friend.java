package com.study.frends.job02;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class Friend implements Writable, DBWritable {

    private String id;
    private Integer loginUserId;
    private String loginUser;
    private Integer friendId;
    private String friend;
    private Integer intimacy;
    private Date createDate;
    private Date updateDate;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeInt(this.loginUserId);
        out.writeUTF(this.loginUser);
        out.writeInt(this.friendId);
        out.writeUTF(this.friend);
        out.writeInt(this.intimacy);
        out.writeLong(this.createDate.getTime());
        out.writeLong(this.updateDate.getTime());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.loginUserId = in.readInt();
        this.loginUser = in.readUTF();
        this.friendId = in.readInt();
        this.friend = in.readUTF();
        this.intimacy = in.readInt();
        this.createDate = new Date(in.readLong());
        this.updateDate = new Date(in.readLong());
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.id);
        statement.setInt(2, this.loginUserId);
        statement.setString(3, this.loginUser);
        statement.setInt(4, this.friendId);
        statement.setString(5, this.friend);
        statement.setInt(6, this.intimacy);
        statement.setTimestamp(7, new Timestamp(this.createDate.getTime()));
        statement.setTimestamp(8, new Timestamp(this.updateDate.getTime()));
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getString(1);
        this.loginUserId = resultSet.getInt(2);
        this.loginUser = resultSet.getString(3);
        this.friendId = resultSet.getInt(4);
        this.friend = resultSet.getString(5);
        this.intimacy = resultSet.getInt(6);
        this.createDate = resultSet.getTimestamp(7);
        this.updateDate = resultSet.getTimestamp(8);
    }

}
