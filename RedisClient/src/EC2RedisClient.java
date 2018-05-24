import java.io.IOException;
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
			fh = new FileHandler("/home/ec2-user/RedisClient/RedisClient/src/EC2Logger.log");
			SimpleFormatter formatter = new SimpleFormatter();  
	        fh.setFormatter(formatter);
	        logger.addHandler(fh);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		// Boolean used to print to log only once
		boolean previouslyFailing = false;

		// ElastiCache Redis Cluster
		String host = "testcluster.iusf0x.ng.0001.use1qa.cache.amazonaws.com";
		int port = 6379;
		
		// Create new master on given host and port to write to
		master = new Jedis(host, port);

		int counter = 0;

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
				master = new Jedis(host, port);
			}
		}				
	}

}
