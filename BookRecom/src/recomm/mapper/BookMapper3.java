package recomm.mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.HashMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BookMapper3 extends Mapper<LongWritable, Text, Text, Text> {
	// HashMap to store the <book_name> of every <item_id>
	private HashMap<String, String> bookMap = new HashMap<String, String>();
	// For Decimal formating double values to two decimal point
	final DecimalFormat df = new DecimalFormat("0.00");

	// Map-side join is performed to load the book_name from second data set
	protected void setup(Context context) throws IOException, InterruptedException {

		// Retrieve the path of the second input data set i.e 'book_data.csv' from
		// context as URI
		URI[] cacheFiles = context.getCacheFiles();
		// validate the cacheFile URI
		if (cacheFiles != null && cacheFiles.length > 0) {
			try {
				// Use FileReader to read the 'book_data.csv' file from the URI path
				BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].getPath()));
				String line;
				// iterate every line of the file
				while ((line = reader.readLine()) != null) {
					// split the <item_id> and <book_name> values from the line
					String[] bookFields = line.split(",");
					// added the <item_id and its <book_name> to the hashMap as key and value
					// respectively
					bookMap.put(bookFields[0], bookFields[1]);
				}
				reader.close();
				// Handle any file reading exceptions
			} catch (IOException e) {
				System.err.println("Error reading book data file: " + e.getMessage());
			}
		}
	}

// Implement the map method
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Format of input 'value': "<item1_id>,<item2_id> <count>,<subRating>"

		// split the input line and obtain the two <item_id>s' and their co-occurrence
		// count and sub-rating
		String[] fields = value.toString().split("\t");
		String[] books = fields[0].split(",");
		String[] vals = fields[1].split(",");
		double count = Double.parseDouble(vals[0]);
		double subRatingSum = Double.parseDouble(vals[1]);

		// Calculate 'likability' percentage between two books by subRating 
		double likability = ((subRatingSum - (double) count) / (count * 4.0)) * 100; // [0-100]

		// Validating the <item_id> and check if it present in the 'bookMap' generated
		// using map-side join
		if (books[0].length() > 3 && bookMap.containsKey(books[0]) && bookMap.containsKey(books[1])) {
			// loading the <book_name> of the corresponding <item_id>
			String bookName1 = bookMap.get(books[0]);
			String bookName2 = bookMap.get(books[1]);

			// set <book_name> as key and the pair <book_name> and its <likability> and <pairCount> score in
			// value to group them by individual book_name
			context.write(new Text(bookName1),
					new Text(bookName2 + "," + df.format(likability) + "," + Long.parseLong(vals[0])));

		}

	}
}
