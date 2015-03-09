package es.index.writes;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;

public abstract class ConfigureClient {

	static Client setupClient(String esClusterName, String esHostName,
			int esPortNum) {

		// Connects to Remote Client defined by the esHostName and Cluster
		// defined by esClusterName
		@SuppressWarnings("unused")
		Node node = nodeBuilder().node();

		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", esClusterName).build();

		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(esHostName,
						esPortNum));
		return client;

	}

	static Logger setupLog(String logFileName, String className) {
		Logger log = Logger.getLogger(className);
		FileHandler fh;
		try {
			fh = new FileHandler(logFileName);
			log.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return log;

	}

	static void closeClient(Client client) {
		// Closing
		client.close();
	}

}
