package org.pingcap.tikv_raw_java;

import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

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
	public static void main(String [] args) {
		RawApiSender.init("172.16.4.88:2384,172.16.4.89:2384,172.16.4.91:2384");
		RawApiSender rawApiSender = new RawApiSender();
		rawApiSender.sendRawApi("k1","v4");
		rawApiSender.sendRawApiDelete("kstring1");
		ByteString value = client.get(ByteString.copyFrom("kstring1".getBytes(StandardCharsets.UTF_8)));
		System.out.println(value.toStringUtf8() + "  end");
	}
}