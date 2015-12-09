package storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountTopology {
	/**
	 * @author simon
	 *
	 */
	public static class SplitSentence extends BaseBasicBolt {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			try {
				String msg = input.getString(0);
				System.out.println("-------------------msg:"+msg + "-------------------");
				if (msg != null) {
					String[] s = msg.split(" ");
					for (String string : s) {
						collector.emit(new Values(string));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}
	/**
	 * @author simon
	 *
	 */
	public static class WordCount extends BaseBasicBolt {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		Map<String, Integer> counts = new HashMap<String, Integer>();

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String word = tuple.getString(0);
			Integer count = counts.get(word);
			if (count == null)
				count = 0;
			count++;
			counts.put(word, count);
			System.out.println("------------------word:"+word+" & count:"+count+"------------------");
			collector.emit(new Values(word, count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
//			declarer.declare(new Fields("word", "count"));
			declarer.declare(new Fields("word"));
		}

		@Override
		public void cleanup() {
			System.out
					.println("------------------ count result ------------------");
			List<String> list = new ArrayList<String>();
			list.addAll(this.counts.keySet());
			for (String sKey : list) {
				System.out.println(sKey + " : " + this.counts.get(sKey));
			}
			System.out
					.println("---------------------------------------------------");
		}
	}

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("spout", new RandomSentenceSpout(), 2);

		builder.setBolt("split", new SplitSentence(), 2).shuffleGrouping(
				"spout");
		
//		builder.setBolt("count", new WordCount(), 2).fieldsGrouping("split",
//				new Fields("word"));
		
		builder.setBolt("count", new WordCount(), 2).shuffleGrouping("split");
		Config conf = new Config();
//		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
//			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count", conf, builder.createTopology());
			Thread.sleep(10000);
			cluster.killTopology("word-count");
			cluster.shutdown();
		}
	}
}
