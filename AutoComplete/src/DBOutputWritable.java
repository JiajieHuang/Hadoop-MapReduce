import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements Writable, DBWritable{
	private String phrase;
	private int count;
	public DBOutputWritable(String phrase, int count) {
		this.phrase=phrase;
		this.count=count;
	}
	@Override
	public void readFields(ResultSet rs) throws SQLException {
		phrase = rs.getString(1);
		count = rs.getInt(2);
	}

	@Override
	public void write(PreparedStatement ps) throws SQLException {
		ps.setString(1, phrase);
		ps.setInt(2, count);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
