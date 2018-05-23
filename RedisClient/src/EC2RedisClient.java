import java.util.Map;
import java.util.List;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class EC2RedisClient {
	
	static Jedis sentinel;
	static Jedis master;
	
	public static void main(String[] args) {
		// Logging info created to log failover response to file
		FileHandler fh;
		final Logger logger = Logger.getLogger(EC2RedisClient.class.getName());
		try {
			fh = new FileHandler("/home/ec2-user/RedisClient/RedisClient/src/logger.log");
			//fh = new FileHandler("/Users/bmouser/Documents/RedisClient/logger.log");
			SimpleFormatter formatter = new SimpleFormatter();  
	        fh.setFormatter(formatter);
	        logger.addHandler(fh);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		// Used for Sentinels
		String localhost = "127.0.0.1";

		// Elasticache Redis NonClustered
		String host = "testcluster.iusf0x.ng.0001.use1qa.cache.amazonaws.com";
		int port = 6379;
		
		// Create a list of sentinels that are currently running on the master at their given ports
		Set<String> sentinels = new HashSet<String>();
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		//jedisClusterNodes.add(new HostAndPort("noncluster.bocodh.ng.0001.usw2.cache.amazonaws.com", 6379));
		//jedisClusterNodes.add(new HostAndPort("127.0.0.1", 6379));
		//		JedisCluster cluster = new JedisCluster(jedisClusterNodes);
		master = new Jedis(host, port);
		//sentinel = new Jedis(localhost,5000);
		//		sentinels.add("127.0.0.1:5000");
		//sentinels.add("127.0.0.1:5001");
		//sentinels.add("127.0.0.1:5002");
		
		// Create a sentinel pool and access the master via the sentinel pool
		//		JedisSentinelPool sentinel_pool = new JedisSentinelPool("mymaster",sentinels);
		//master = sentinel_pool.getResource();
		
		// PubSub to listen for failover and log messages from failover
		final JedisPubSub sub = new JedisPubSub() {
		@Override
		public void onPMessage(String pattern, String channel, String message) {
			logger.log(Level.INFO, channel);
			logger.log(Level.INFO, message);
			}
		};
		// Start the sentinel PUBSUB on new thread because it is blocking.
		new Thread(new Runnable() {
			@Override
			public void run() {
			    //	    			sentinel.psubscribe(sub, "*");
			}
			
		}).start();

		//List<Map<String, String>> slaves = sentinel.sentinelSlaves("mymaster");
		//String slaveHost = slaves.get(0).get("ip");
		//String slavePort = slaves.get(0).get("port");
		//System.out.println(slaveHost);
		//System.out.println(slavePort);
		//Jedis slave = new Jedis(slaveHost, Integer.parseInt(slavePort));

		int counter = 0;
		boolean previouslyFailing = false;
		// Continually write message to the server, then cause failover from outside of client and watch how client responds.
		while(true) {
		    try{
			String response = master.set(Integer.toString(counter), Integer.toString(counter));
			if(response.equals("OK")) {
			    counter++;
			    System.out.print("\rSuccessful");
			}
			if(counter >= 30000){
			    counter = 0;
			}
			if(previouslyFailing) {
			    previouslyFailing = false;
			    logger.log(Level.WARNING, "Automatic Failover: Failover Finished, Currently Writing to Master");
			}
			}
			catch(JedisConnectionException jce) {
				System.out.print("Failover Occuring\n");
				if(!previouslyFailing)
				    logger.log(Level.WARNING, "Automatic Failover: Failover Occuring, Unable to Write to Master");
				previouslyFailing = true;
				master = new Jedis("testcluster.iusf0x.ng.0001.use1qa.cache.amazonaws.com", 6379);
			}
		}				
	}

}
