package org.heigit.bigspatialdata.oshdb.etl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.heigit.bigspatialdata.oshdb.etl.extract.data.KeyValuesFrequency;
import org.heigit.bigspatialdata.oshdb.util.bytearray.ShareInOutBuffer;
import org.heigit.bigspatialdata.oshdb.util.bytearray.UnsynchronByteArrayInputStream;
import org.heigit.bigspatialdata.oshdb.util.bytearray.UnsynchronByteArrayOutputStream;

public class HOSMExtract2 {

	private static final int MB = 1024 * 1024;

	private static final int SHARE_BUFFER_SIZE = 10*MB;
	private static final int MAX_KV_BUFFER_SIZE = SHARE_BUFFER_SIZE;

	ShareInOutBuffer[] shareKVBuffers = new ShareInOutBuffer[] { //
			new ShareInOutBuffer(SHARE_BUFFER_SIZE), //
			new ShareInOutBuffer(SHARE_BUFFER_SIZE) };

	int keyValueStream = 0;


	DataInputStream[] keyValueInputStreams = new DataInputStream[] {
			new DataInputStream(new UnsynchronByteArrayInputStream(shareKVBuffers[0])),
			new DataInputStream(new UnsynchronByteArrayInputStream(shareKVBuffers[1]))};
	
	DataOutputStream[] keyValueOuputStreams = new DataOutputStream[] {
			new DataOutputStream(new UnsynchronByteArrayOutputStream(shareKVBuffers[1])),
			new DataOutputStream(new UnsynchronByteArrayOutputStream(shareKVBuffers[0]))};

	DataInputStream keyValueInputStream = null;
	DataOutputStream keyValueOuputStream = null;

	private void extract(SortedMap<String, KeyValuesFrequency> kvFrequencies) throws IOException {

		mergKeyValuesFrequencies(kvFrequencies);

	}

	private void mergKeyValuesFrequencies(SortedMap<String, KeyValuesFrequency> kvFrequencies) throws IOException {
		ShareInOutBuffer shareKVBufferIn = shareKVBuffers[keyValueStream];
		keyValueInputStream = keyValueInputStreams[keyValueStream];
		keyValueOuputStream = keyValueOuputStreams[keyValueStream];
		
		for (Map.Entry<String, KeyValuesFrequency> newEntry : kvFrequencies.entrySet()) {
			String newKey = newEntry.getKey();
			KeyValuesFrequency newKVFrequency = newEntry.getValue();

			Map.Entry<String, KeyValuesFrequency> currentEntry = readKeyValueFrequency();

			int c = (currentEntry != null) ? newEntry.getKey().compareTo(currentEntry.getKey()) : -1;
			while (newEntry != null && c > 0) {
				writeKeyValueFrequency(currentEntry.getKey(), currentEntry.getValue());
				currentEntry = readKeyValueFrequency();
				c = (currentEntry != null) ? newEntry.getKey().compareTo(currentEntry.getKey()) : -1;
			}
			if (c < 0) {
				// we got a new key entry
				writeKeyValueFrequency(newKey, newKVFrequency);
			}
			if (c == 0) {
				String currentKey = currentEntry.getKey();
				KeyValuesFrequency currentKVFrequncy = currentEntry.getValue();

				SortedMap<String, Integer> currentValues = currentKVFrequncy.values();
				for (Map.Entry<String, Integer> newValueEntry : newKVFrequency.values().entrySet()) {
					Integer f = currentValues.get(newValueEntry.getKey());
					f = (f == null) ? newValueEntry.getValue()
							: (Integer.valueOf(f.intValue() + newValueEntry.getValue().intValue()));
					currentValues.put(newValueEntry.getKey(), f);
				}
				newKVFrequency = new KeyValuesFrequency(currentKVFrequncy.freq() + newKVFrequency.freq(),
						currentValues);
				writeKeyValueFrequency(currentKey, newKVFrequency);
			}
		}
		
		keyValueStream = (keyValueStream+1)%2;
		ShareInOutBuffer shareKVBufferOut = shareKVBuffers[keyValueStream];

		if (shareKVBufferOut.size() > MAX_KV_BUFFER_SIZE) {
			// write out the last outputbuffer and reset
			//shareKVBuffer.writeTo(out);
			shareKVBuffers[0].reset();
			shareKVBuffers[1].reset();
		}else{
			shareKVBufferIn.reset();
		}
	}

	private Map.Entry<String, KeyValuesFrequency> readKeyValueFrequency() throws IOException {
		if (keyValueInputStream.available() > 0) {
			String key = keyValueInputStream.readUTF();
			int freq = keyValueInputStream.readInt();

			int size = keyValueInputStream.readInt();
			SortedMap<String, Integer> values = new TreeMap<>();
			for (int i = 0; i < size; i++) {
				values.put(keyValueInputStream.readUTF(), Integer.valueOf(keyValueInputStream.readInt()));
			}
			KeyValuesFrequency kvc = new KeyValuesFrequency(freq, values);

			return new AbstractMap.SimpleEntry<String, KeyValuesFrequency>(key, kvc);
		}
		return null;
	}

	private void writeKeyValueFrequency(String key, KeyValuesFrequency kvf) throws IOException {
		System.out.printf("write: %s\n",key);
		keyValueOuputStream.writeUTF(key);
		System.out.printf("write: %s\n",kvf.freq());
		keyValueOuputStream.writeInt(kvf.freq());
		System.out.printf("write: %s\n",kvf.values().size());
		keyValueOuputStream.writeInt(kvf.values().size());
		for (Map.Entry<String, Integer> entry : kvf.values().entrySet()) {
			System.out.printf("write: %s\n",entry.getKey());
			keyValueOuputStream.writeUTF(entry.getKey());
			System.out.printf("write: %s\n",entry.getValue().intValue());
			keyValueOuputStream.writeInt(entry.getValue().intValue());
		}
	}

	public static void main(String[] args) {
		HOSMExtract2 hosm = new HOSMExtract2();
		
		try {
			
			SortedMap<String,Integer> vFrequencies;
			SortedMap<String, KeyValuesFrequency> kvFrequencies; 
			
			
			System.out.println();
			vFrequencies = new TreeMap<>();
			vFrequencies.put("Troilo", 1);
			kvFrequencies = new TreeMap<>();
			kvFrequencies.put("name", new KeyValuesFrequency(1, vFrequencies));
			hosm.extract(kvFrequencies);
			
			System.out.println();
			vFrequencies = new TreeMap<>();
			vFrequencies.put("Raifer", 1);
			kvFrequencies = new TreeMap<>();
			kvFrequencies.put("name", new KeyValuesFrequency(1, vFrequencies));
			hosm.extract(kvFrequencies);
			
			System.out.println();
			vFrequencies = new TreeMap<>();
			vFrequencies.put("yes", 1);
			kvFrequencies = new TreeMap<>();
			kvFrequencies.put("building", new KeyValuesFrequency(1, vFrequencies));
			hosm.extract(kvFrequencies);
			
			System.out.println();
			vFrequencies = new TreeMap<>();
			vFrequencies.put("Troilo", 1);
			kvFrequencies = new TreeMap<>();
			kvFrequencies.put("name", new KeyValuesFrequency(1, vFrequencies));
			hosm.extract(kvFrequencies);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
