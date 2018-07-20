package com.youtube.etl;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.youtube.utl.ETLUtils;


/** 
 * @Description：将movie数据清洗成需要的数据
 * <p>创建日期：2018年7月20日 </p>
 * @version V1.0  
 * @author Administrator
 * @see
 */
public class ETLYoutubeVideoMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	Text text=new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		String string = value.toString();
		String etlString = ETLUtils.getETLString(string);
		if (StringUtils.isNotBlank(etlString)) {
			text.set(etlString);
			context.write(text, NullWritable.get());
		}
	}

}
