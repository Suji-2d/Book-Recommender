package recomm.reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Inheriting the Reducer class and the reducer output key-value types are provided
public class BookReducer1 extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// list of type String is created
		ArrayList<String> bookList = new ArrayList<>();

		// Format of input 'key - values': "<user_id> - [<item_id>,<rating>,...]"
		
		// 'values' containing list of every '<item_id>,<rating>' of single <user_id>
		// i.e key is iterated
		for (Text value : values) {
			// the '<item_id>,<rating>' is appended to the 'bookList'
			bookList.add(value.toString());
		}

		// the <user_id> is set as key and the list of '<item_id>,<rating>' for the
		// respective user is sent as value
		context.write(key, new Text(String.join(",", bookList)));

	}
}