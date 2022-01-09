package org.pingcap.tikv_raw_java;

import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;

public class RawApiSender {
	static RawKVClient client = null;
	public static void init(String pdAddress) {
		if (client == null) {
			TiConfiguration conf = TiConfiguration.createRawDefault(pdAddress);
			TiSession session = TiSession.create(conf);
			client = session.createRawClient();
		}
	}
	public boolean sendRawApi(String key, String value) {
		if (client != null) {
			client.put(ByteString.copyFrom(key.getBytes(StandardCharsets.UTF_8)), ByteString.copyFrom(value.getBytes(StandardCharsets.UTF_8)));
			return true;
		}
		return false;
	}
	public boolean sendRawApiDelete(String key) {
		if (client != null) {
			client.delete(ByteString.copyFrom(key.getBytes(StandardCharsets.UTF_8)));
			return true;
		}
		return false;
	}
	public static void main(String [] args) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader("/tmp/tidb/kv"));
		RawApiSender.init("172.16.4.81:42379,172.16.4.88:42379,172.16.4.88:42379");
		RawApiSender rawApiSender = new RawApiSender();
        String line;
        while((line = bufferedReader.readLine()) != null) {
            String [] arr = line.split("#");
			if (arr.length == 3) {
				String key = arr[0];
				String value = arr[1];
				String type = arr[2];
				System.out.println(String.format("key is %s, value is %s, type is %s", key, value, type));
				if (type.equals("INSERT")) {

					rawApiSender.sendRawApi(key, value);
				} else {
					rawApiSender.sendRawApiDelete(key);
				}
			}
        }
        bufferedReader.close();
	}
}