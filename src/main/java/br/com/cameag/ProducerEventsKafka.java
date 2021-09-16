package br.com.cameag;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerEventsKafka {
	
	private static final KafkaProducer<String, String> _producer = new KafkaProducer<String, String>(getPropertiesProducer("localhost:9092", "all"));
	
	public static Properties getPropertiesProducer(String server,String acks) {
		Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", server);
        kafkaProps.put("acks", acks);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return kafkaProps;
	}
	
	public static byte[] serializer(GenericRecord user,Schema schema) throws Exception{
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		
		DatumWriter<GenericRecord> userDatumWriter = new SpecificDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(userDatumWriter);
		
		dataFileWriter.create(user.getSchema(), outputStream);
		dataFileWriter.append(user);
		dataFileWriter.close();
		
		return outputStream.toByteArray();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception{
		
		Schema schema = new Schema.Parser().parse(new File("user.avsc"));
		
		List<GenericRecord> lista = new ArrayList<GenericRecord>();
		
		GenericRecord user1 = new GenericData.Record(schema);
		user1.put("name", "Alyssa");
		user1.put("favorite_number", 256);
		lista.add(user1);

		GenericRecord user2 = new GenericData.Record(schema);
		user2.put("name", "Ben");
		user2.put("favorite_number", 7);
		user2.put("favorite_color", "red");
		lista.add(user2);
		
		GenericRecord user3 = new GenericData.Record(schema);
		user3.put("name", "Cysse");
		user3.put("favorite_number", 2);
		user3.put("favorite_color", "black");
		lista.add(user3);
		
		GenericRecord user4 = new GenericData.Record(schema);
		user4.put("name", "Mike");
		user4.put("favorite_number", 200);
		lista.add(user4);
		
		GenericRecord user5 = new GenericData.Record(schema);
		user5.put("name", "Vicent");
		user5.put("favorite_number", 3);
		lista.add(user5);
		
		
		while (true) { 
			byte[] objSerializer = serializer((GenericRecord)lista.get(new Random().nextInt(5)),schema);
			System.out.println("Enviando Byte Array... "+ objSerializer);
			_producer.send(new ProducerRecord("quickstart-events", objSerializer));
			Thread.sleep(5000);
		}
	}
}
