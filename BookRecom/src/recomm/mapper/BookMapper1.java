package recomm.mapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Inheriting the Mapper class and the mapper's output key-value types are provided
public class BookMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	// Implement the map method
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Format of input 'value': "<user_id>,<item_id>,<rating>"

		// converting the 'value' to Java String and splitting them to get individual
		// fields
		String[] ratingFields = value.toString().split(",");

		// <User_id> is set as key and the concatenated value of <item_id> and <rating>
		// by "," is set as value
		context.write(new Text(ratingFields[1]), new Text(ratingFields[0] + "," + ratingFields[2]));
	}
}
