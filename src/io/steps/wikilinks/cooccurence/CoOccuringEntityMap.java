package io.steps.wikilinks.cooccurence;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CoOccuringEntityMap extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private static Text pair = new Text();
	private static IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] lines = value.toString().split("\n");
		List<String> mentions = new LinkedList<String>();

		for (String line : lines) {
			if (line.startsWith("MENTION")) {
				mentions.add(line.split("\t")[3]);
			}
		}

		for (String mentionA : mentions) {
			for (String mentionB : mentions) {
				if (mentionA.toString().compareTo(mentionB.toString()) < 0) {
					pair.set(mentionA + "," + mentionB);
					context.write(pair, one);
				}
			}
		}
	}
}
