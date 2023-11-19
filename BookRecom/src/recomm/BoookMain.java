package recomm;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import recomm.mapper.BookMapper1;
import recomm.mapper.BookMapper2;
import recomm.mapper.BookMapper3;
import recomm.reducer.BookReducer1;
import recomm.reducer.BookReducer2;
import recomm.reducer.BookReducer3;


public class BoookMain {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		JobControl control = new JobControl("My Job Chain");

		if (args.length != 6) {
			System.err.println(
					"Usage: BookMain <input1 path> <input2 path> "
					+ "<output1 path> <output2 path> <output3 path>");
			System.exit(-1);
		}

		// Set up Job1
		Job job1;
		job1 = Job.getInstance(conf, "Book CF RS 1");
		job1.setJarByClass(BoookMain.class);
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[3]));
		job1.setMapperClass(BookMapper1.class);
		job1.setReducerClass(BookReducer1.class);
		job1.setCombinerClass(BookReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		// Delete output if exists
		FileSystem hdfs1 = FileSystem.get(conf);
		Path outputDir1 = new Path(args[3]);
		if (hdfs1.exists(outputDir1))
			hdfs1.delete(outputDir1, true);

		// Set up Job2
		Job job2;
		job2 = Job.getInstance(conf, "Book CF RS 2");
		job2.setJarByClass(BoookMain.class);
		FileInputFormat.addInputPath(job2, new Path(args[3] + "/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(args[4]));
		job2.setMapperClass(BookMapper2.class);
		job2.setReducerClass(BookReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		// Delete output if exists
		FileSystem hdfs2 = FileSystem.get(conf);
		Path outputDir2 = new Path(args[4]);
		if (hdfs2.exists(outputDir2))
			hdfs2.delete(outputDir2, true);

		// Set up Job3
		Job job3;
		job3 = Job.getInstance(conf, "Book CF RS 3");
		job3.setJarByClass(BoookMain.class);
		FileInputFormat.addInputPath(job3, new Path(args[4] + "/part-r-00000"));
		FileOutputFormat.setOutputPath(job3, new Path(args[5]));
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		// set input2 path for map-side join operation
		job3.addCacheFile(new URI(args[2]));

		// Set mapper and reducer classes
		job3.setMapperClass(BookMapper3.class);
		job3.setReducerClass(BookReducer3.class);
		// job1.setCombinerClass(BookReducer1.class);

		// Delete output if exists
		FileSystem hdfs3 = FileSystem.get(conf);
		Path outputDir3 = new Path(args[3]);
		if (hdfs3.exists(outputDir3))
			hdfs3.delete(outputDir3, true);

		// Create ControlledJob for every Jobs
		ControlledJob cJob1 = new ControlledJob(job1.getConfiguration());
		ControlledJob cJob2 = new ControlledJob(job2.getConfiguration());
		ControlledJob cJob3 = new ControlledJob(job3.getConfiguration());

		// Add the jobs to the JobControl object
		control.addJob(cJob1);
		control.addJob(cJob2);
		control.addJob(cJob3);

		// Define the dependency between the jobs
		cJob2.addDependingJob(cJob1);
		cJob3.addDependingJob(cJob2);

		// Submit the JobControl object to the cluster
		Thread jobControlThread = new Thread(control);
		jobControlThread.start();
		jobControlThread.join();

		System.exit(job3.waitForCompletion(true) ? 0 : 1);

	}
}
