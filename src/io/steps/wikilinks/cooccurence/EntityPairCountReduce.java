package io.steps.wikilinks.cooccurence;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EntityPairCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	private static IntWritable count = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		Iterator<IntWritable> it = values.iterator();
		int sum = 0;
		while (it.hasNext()) {
			sum += it.next().get();
		}
		count.set(sum);
		context.write(key, count);
	}
}
