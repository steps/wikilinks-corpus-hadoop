package io.steps.wikilinks.cooccurence;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EntityMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static Text mention = new Text();
	private static IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] lines = value.toString().split("\n");
		for (String line : lines) {
			if (line.startsWith("MENTION")) {
				mention.set(line.split("\t")[3]);
				context.write(mention, one);
			}
		}
	}

}
