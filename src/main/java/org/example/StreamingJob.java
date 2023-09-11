package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// 创建Flink执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 创建一个DataStream表示输入数据
		DataStream<String> inputData = env.fromElements(
				"[1,2312]",
				"[1,21321]",
				"[2,1321]",
				"[2,412]",
				"[1,1241]",
				"[2,213]",
				"[3,1241]"
		);

		// 使用map操作将输入数据解析为Tuple2<Integer, Integer>
		DataStream<Tuple2<Integer, Integer>> parsedData = inputData.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> map(String value) throws Exception {
				String[] parts = value.substring(1, value.length() - 1).split(",");
				int index = Integer.parseInt(parts[0]);
				int number = Integer.parseInt(parts[1]);
				return Tuple2.of(index, number);
			}
		});

		// 使用keyBy操作根据索引进行分组，然后计算每个分组的总和
		DataStream<Tuple2<Integer, Integer>> aggregatedData = parsedData
				.keyBy(0) // 使用索引0进行分组
				.reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1)); // 计算每个分组的总和

		// 打印聚合后的结果
		aggregatedData.print();

		// 执行Flink程序
		env.execute("Index Aggregation");
	}
}
