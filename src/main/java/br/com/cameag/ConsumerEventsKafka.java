package br.com.cameag;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerEventsKafka {
	
	private static final KafkaConsumer<String, byte[]> _consumer = new KafkaConsumer<String, byte[]>(getPropertiesConsumer("localhost:9092", "all", "test"));

	public static Properties getPropertiesConsumer(String server,String acks,String grupo) {
		Properties props = new Properties();
        props.put("bootstrap.servers", server); 
        props.put("acks", acks);
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("group.id", grupo);
        return props;
	}
	
	public static GenericRecord deserializer(byte[] myBytes,Schema schema) throws Exception{
		
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		@SuppressWarnings("resource")
		DataFileStream<GenericRecord> dataFileStream = new DataFileStream<GenericRecord>(new ByteArrayInputStream(myBytes), datumReader);
		return dataFileStream.next();
	}
	
	public static void main(String[] args) throws Exception{
		
		Schema schema = new Schema.Parser().parse(new File("user.avsc"));
		
		_consumer.subscribe(Arrays.asList("quickstart-events"));
		  
		while (true) { 
			ConsumerRecords<String, byte[]> records = _consumer.poll(100);
			for (ConsumerRecord<String, byte[]> record : records){
				System.out.println(deserializer(record.value(),schema) +" - offset -> "+ record.offset());
				_consumer.commitSync();
			} 
		}
	}
}
