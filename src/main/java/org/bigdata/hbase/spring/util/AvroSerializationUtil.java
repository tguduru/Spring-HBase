package org.bigdata.hbase.spring.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * 
 * @author ghstreddy
 *
 */
public class AvroSerializationUtil {
	Schema schema = null;

	public byte [] serialize() throws IOException{
		schema = new Schema.Parser().parse(AvroSerializationUtil.class.getResourceAsStream("/user.avsc"));
		GenericRecord user = new GenericData.Record(schema);
		user.put("firstName", "THIRUPATHI REDDY");
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Encoder e = new EncoderFactory().binaryEncoder(out, null);
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);

		writer.write(user, e);
		e.flush();
		ByteBuffer bytes = ByteBuffer.allocate(out.toByteArray().length);
		bytes.put(out.toByteArray());
		return bytes.array();
	}

	public void deserialize(byte[] data) throws IOException{
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
		Decoder in = new DecoderFactory().binaryDecoder(data, null);
		GenericRecord record = reader.read(null, in);
		System.out.println("Deserialized");
		System.out.println(record);
	}

	public static void main(String [] args) throws IOException{
		AvroSerializationUtil avro = new AvroSerializationUtil();
		byte [] data =avro.serialize();
		avro.deserialize(data);
	}

}
