package recomm.reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.stream.DoubleStream;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Inheriting the Reducer class and the reducer output key-value types are provided
public class BookReducer2 extends Reducer<Text, Text, Text, Text> {

	// For Decimal formating double values to two decimal point
	final DecimalFormat df = new DecimalFormat("0.00");

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// Format of input 'key - values': "<item_id>,<item_id> - [<rating>,...]"
		
		// Initialise count and subratingSum to 0;
		long count = 0;
		Double subRatingSum = 0.0;

		// 'values' containing list of every '<subRating>' for every its pair of
		// <item_id>s'
		// i.e key

		// the <item_id> pair is split into separate <item_id>s'
		String[] bookPair = key.toString().split(",");

		String book1 = bookPair[0].trim();
		String book2 = bookPair[1].trim();

		// the 'values' containing list of <subRating> is iterated and the co-occurrence
		// count of a pair of
		// <item_id> and the sum of their <subRating> are calculated
		for (Text value : values) {
			count++;
			subRatingSum += Double.parseDouble(value.toString());
		}

		// Removing <item_id>s' having fewer co-occurrence count i.e 10 and validating item_id 
		if (count > 10 && book2.length() > 3 && !book1.equals(book2))
			// the <item_id> is set as key and its pair '<item_id>,<subRating>' is sent as value
			context.write(key, new Text(count + "," + df.format(subRatingSum)));

	}

}
