package io.steps.wikilinks.cooccurence;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EntityCountReduce extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private static Text entity = new Text();
	private static IntWritable count = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		String entityString = key.toString();
		Iterator<IntWritable> it = values.iterator();
		int sum = 0;
		while (it.hasNext()) {
			sum += it.next().get();
		}
		entity.set(entityString);
		count.set(sum);
		context.write(entity, count);
	}
}
