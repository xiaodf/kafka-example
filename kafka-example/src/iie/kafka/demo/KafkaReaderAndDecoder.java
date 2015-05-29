package iie.kafka.demo;

import iie.kafka.topics.HIVETopicGenerator;
import iie.kafka.topics.Topic;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

public class KafkaReaderAndDecoder extends Thread {
	private final ConsumerConnector consumer;
	private final String topic;

	public KafkaReaderAndDecoder(String topic) {
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig());
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", "192.168.8.103:2181/kafka");
		props.put("group.id", "demo0111");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}

	public String decoderKafkaData(String topicName, byte[] bytes) {
		StringBuilder sb = new StringBuilder();
		Topic topic;
		try {
			topic = new HIVETopicGenerator("thrift://m103:9083").getTopic(topicName);
			System.out.println(topic.getSchema().getFields().size());
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
					topic.getSchema());
			Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
			GenericRecord record;
			record = reader.read(null, decoder);
			topic.getSchema();
			int fieldSize = record.getSchema().getFields().size();
			if (fieldSize > 0) {
				sb.append(record.get(0)).append(";");
				for (int i = 1; i < fieldSize; i++) {
					sb.append(record.get(i)).append(";");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			System.out.println(decoderKafkaData(topic, it.next().message()));
		}
		consumer.commitOffsets();
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.exit(0);
		}
		KafkaReaderAndDecoder t1 = new KafkaReaderAndDecoder(args[0]);
		t1.start();
	}
}