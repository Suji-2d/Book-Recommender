package recomm.mapper;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Inheriting the Mapper class and the mapper's output key-value types i.e 'Text' are provided
public class BookMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	// Implement the map method
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// For Decimal formating double values to two decimal point
		final DecimalFormat df = new DecimalFormat("0.00");

		// Format of input 'value': "<user_id> <item1_id>,<rating1>,<item2_id>,<rating2>..."

		// the 'value' is split by '\t' to get the <user_id> and its corresponding list
		// of '<item_id>,<rating>'
		String[] fields = value.toString().split("\t");

		// the list of '<item_id>,<rating>' is then split by ',' and set into a string
		// array
		String[] bookList = fields[1].split(",");

		// HashMap is created
		HashMap<String, Double> bookMap = new HashMap<>();

		// the 'bookList' array is iterated, <item_id> and its <rating> are added as key
		// and value respectively
		for (int i = 0; i < bookList.length - 1; i++) {
			bookMap.put(bookList[i].trim(), Double.parseDouble(bookList[i + 1]));
		}

		// the 'bookMap' containing every <item_id> as key and its <rating> as value for
		// a <user_id> is
		// then double iterated to obtain the all the *combination of two <item_id>*
		for (Map.Entry<String, Double> entry1 : bookMap.entrySet()) {
			String book1 = entry1.getKey();
			int hash1 = System.identityHashCode(book1);
			Double rating1 = entry1.getValue();
			for (Map.Entry<String, Double> entry2 : bookMap.entrySet()) {
				String book2 = entry2.getKey();
				if (hash1 > System.identityHashCode(book2))
					continue;
				Double rating2 = entry1.getValue();

				// the key is the concatenated value of a combination of two <item_id>s' and the
				// value is their calculated sub-rating
				context.write(new Text(book1 + "," + book2), new Text(df.format(getSubRating(rating1, rating2))));
			}
		}

	}

	// Generate co-occurrence Score i.e sub-rating between two books/<item_id> based
	// on their <rating>s'
	public double getSubRating(double a, double b) {
		// Average between two <rating>s'
		return (a + b) / 2.0;
	}
}
