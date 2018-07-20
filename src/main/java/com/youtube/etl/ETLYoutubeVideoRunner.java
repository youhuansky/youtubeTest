package com.youtube.etl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ETLYoutubeVideoRunner implements Tool {

	Configuration conf = new Configuration();

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		conf = this.getConf();
		conf.set("inpath", args[0]);
		conf.set("outpath", args[1]);

		Job job = Job.getInstance(conf, "youtube-video-etl");

		job.setJarByClass(ETLYoutubeVideoRunner.class);

		job.setMapperClass(ETLYoutubeVideoMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		// 关闭reduce
		job.setNumReduceTasks(0);

		this.initJobInputPath(job);
		this.initJobOutputPath(job);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private void initJobOutputPath(Job job) throws Exception {

		Configuration configuration = job.getConfiguration();
		String pathString = configuration.get("outpath");

		FileSystem fs = FileSystem.get(configuration);
		Path path = new Path(pathString);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}

		FileOutputFormat.setOutputPath(job, path);
	}

	private void initJobInputPath(Job job) throws Exception {

		Configuration configuration = job.getConfiguration();
		String pathString = configuration.get("inpath");

		FileSystem fs = FileSystem.get(configuration);
		Path path = new Path(pathString);

		if (fs.exists(path)) {
			FileInputFormat.setInputPaths(job, path);
		} else {
			throw new RuntimeException("没有找到此文件");
		}

	}

	public static void main(String[] args) throws Exception {

		int run = ToolRunner.run(new ETLYoutubeVideoRunner(), args);

		System.exit(run);
		
	}

}
