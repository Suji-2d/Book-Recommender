package recomm.reducer;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import recomm.dao.BookSorting;

public class BookReducer3 extends Reducer<Text, Text, Text, Text> {
	// For Decimal formating double values to two decimal point
	final DecimalFormat df = new DecimalFormat("0.00");

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// Format of input 'key - values': "<book_name> -
		// ['<book_name>,<likability>',...]"

		// list of type 'BookSorting.class' is created
		ArrayList<BookSorting> bookList = new ArrayList<>();

		// the 'values' containing list of '<book_name>,<likability>' for every
		// <book_name> is iterated
		for (Text value : values) {
			// 'value' is split and individual values are obtained
			String[] fields = value.toString().split(",");
			String book = fields[0];
			double pairCount = Double.parseDouble(fields[2]);
			double likability = Double.parseDouble(fields[1]);

			// the <book_name>, <pairCount> and <likability> is used to create 'BookSorting'
			// object and added to 'bookList'
			bookList.add(new BookSorting(book, pairCount, likability));
		}

		// Sorting the 'bookList' by <likability> in descending order
		Collections.sort(bookList);

		// Calculating the min and max of pairCount over all the co-occurant books
		double max = bookList.get(0).getPairCount();
		double min = bookList.get(bookList.size() - 1).getPairCount();

		// Adding the pairCount factor to the likeability percentage to normalise it
		// between every books and returning single String value containing list of top
		// ten likable books and their likability percentage
		ArrayList<String> topBookList = (ArrayList<String>) bookList.stream()
				.map(bk -> "[ " + bk.getBook() + " - "
						+ df.format(
								((max != min ? ((bk.getPairCount() - min) / (max - min)) : 1)) * bk.getLikeability())
						+ " % ]")
				.limit(10).collect(Collectors.toList());

		// output key-value sample : "<book_name> ==> [<book1_name - <likability> %],
		// [<book2_name - <likability> %], ...
		context.write(key, new Text("==> " + String.join(",", topBookList)));

	}
}