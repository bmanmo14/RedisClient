import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisClient {
	
	static Jedis sentinel;
	static Jedis master;
	
	public static void main(String[] args) {
		// Logging info created to log failover response to file
		FileHandler fh;
		final Logger logger = Logger.getLogger(RedisClient.class.getName());
		try {
			fh = new FileHandler("/Users/bmouser/Documents/RedisClient/logger.log");
			SimpleFormatter formatter = new SimpleFormatter();  
	        fh.setFormatter(formatter);
	        logger.addHandler(fh);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		// Create a list of sentinels that are currently running on the master at their given ports
		Set<String> sentinels = new HashSet<String>();
		sentinel = new Jedis("127.0.0.1",5000);
		sentinels.add("127.0.0.1:5000");
		sentinels.add("127.0.0.1:5001");
		sentinels.add("127.0.0.1:5002");
		
		// Create a sentinel pool and access tqhe master via the sentinel pool
		JedisSentinelPool sentinel_pool = new JedisSentinelPool("mymaster",sentinels);
		master = sentinel_pool.getResource();
		
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
				sentinel.psubscribe(sub, "*");
			}
			}).start();


		boolean previouslyFailing = false;
		// Continually write message to the server, then cause failover from outside of client and watch how client responds.
		while(true) {
		    try{
			String response = master.set("key","value");
			if(response.equals("OK")) {
				System.out.print("\rSuccessful");
			}
			if(previouslyFailing) {
				previouslyFailing = false;
				logger.log(Level.WARNING, "Sentinel: FailoverFinished, Currently Writing to Master");
			}
			}
			catch(JedisConnectionException jce) {
				if(!previouslyFailing) {
					logger.log(Level.WARNING, "Sentinel: FailoverOccuring, Unable to Write to Master");
					System.out.print("\rFailover Occuring");
				}
				previouslyFailing = true;
				master.close();
				master = sentinel_pool.getResource();
			}
		}				
	}

}
