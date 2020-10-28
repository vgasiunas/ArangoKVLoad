package com.arangodb.kvload;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Random;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.ArangoDBVersion;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.velocypack.VPackBuilder;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocypack.ValueType;

public class ArangoKVLoad {

	private final static Random random = new Random();
	
	// options with their default values
	private static String dbName = "bench";
	private static String collectionName = "docs";
	private static int numDocs = 100000;
	private static int numAttrs = 10;
	private static int attrLen = 100;
	private static int batchSize = 50;
	private static int numThreads = 1;
	private static int pctWrite = 50;
	private static int benchTime = 10;
	private static String host = "localhost";
	private static int port = 8529;
	private static String user = "root";
	private static String password = "";
	private static boolean useSSL = false;
	private static String caCertPath = "";
	private static int replicationFactor = 1;
	private static int writeConcern = 1;
	
	private final static int attrDictSize = 1000;
	private final static ArrayList<String> attrDict = new ArrayList<String>();
	
	private static class ThreadStat {
		public long insertTime = 0;
		public long numReads = 0;
		public long numWrites = 0;
	};
	
	private static ThreadStat[] threadStats;
	
	private static String randomString(int length) {
		int leftLimit = 48; // numeral '0'
	    int rightLimit = 122; // letter 'z'
	    return random.ints(leftLimit, rightLimit + 1)
	      .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
	      .limit(length)
	      .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
	      .toString();
	}
	
	private static void populateAttributeDictionary() {
		for (int j = 0; j < attrDictSize; j++) {
			attrDict.add(randomString(attrLen));
		}
	}
	
	private static VPackSlice buildObject(int index) {
		VPackBuilder builder = new VPackBuilder();
		builder.add(ValueType.OBJECT);
		builder.add("_key", "" + index);
		for (int j = 0; j < numAttrs; j++) {
			builder.add("attr" + j, attrDict.get(random.nextInt(attrDictSize)));
		}
		builder.close();
		return builder.slice();
	}

	private static void parseCommandLineOptions(String[] args) {
		Options options = new Options();

		Option option = new Option("d", "database", true, "database name");
        options.addOption(option);

        option = new Option("c", "collection", true, "collection name");
        options.addOption(option);
        
        option = new Option("n", "numdocs", true, "number of documents");
        option.setType(Number.class);
        options.addOption(option);
        
        option = new Option("a", "numattrs", true, "number of attributes");
        option.setType(Number.class);
        options.addOption(option);
        
        option = new Option("l", "attrlen", true, "attribute length");
        option.setType(Number.class);
        options.addOption(option);
        
        option = new Option("t", "threads", true, "number of threads");
        option.setType(Number.class);
        options.addOption(option);
        
        option = new Option("b", "batchsize", true, "batch size");
        option.setType(Number.class);
        options.addOption(option);
        
        option = new Option("T", "time", true, "benchmark time in seconds");
        option.setType(Number.class);
        options.addOption(option);
        
        option = new Option("w", "pctwrite", true, "percentage of write operatins (0-100)");
        option.setType(Number.class);
        options.addOption(option);
        
        option = new Option("h", "host", true, "host name");
        options.addOption(option);
        
        option = new Option("P", "port", true, "port number");
        option.setType(Number.class);
        options.addOption(option);
        
        option = new Option("u", "user", true, "user name");
        options.addOption(option);
        
        option = new Option("p", "password", true, "password");
        options.addOption(option);
        
        option = new Option("S", "usessl", false, "use SSL");
        options.addOption(option);
        
        option = new Option("C", "cacert", true, "path to the CA certificate");
        options.addOption(option);
        
        option = new Option("r", "replfactor", true, "collection replication factor");
        option.setType(Number.class);
        options.addOption(option);
        
        option = new Option("W", "writeconcern", true, "collection write concern");
        option.setType(Number.class);
        options.addOption(option);


        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        
	        if (cmd.hasOption("database")) {
	        	dbName = cmd.getOptionValue("database");
	        }
	        
	        if (cmd.hasOption("collection")) {
	        	collectionName = cmd.getOptionValue("collection");
	        }
	        
	        if (cmd.hasOption("numdocs")) {
	        	numDocs = ((Number)cmd.getParsedOptionValue("numdocs")).intValue();
	        }
	        
	        if (cmd.hasOption("numattrs")) {
	        	numAttrs = ((Number)cmd.getParsedOptionValue("numattrs")).intValue();
	        }
	        
	        if (cmd.hasOption("attrlen")) {
	        	attrLen = ((Number)cmd.getParsedOptionValue("attrlen")).intValue();
	        }
	        
	        if (cmd.hasOption("threads")) {
	        	numThreads = ((Number)cmd.getParsedOptionValue("threads")).intValue();
	        }
	        
	        if (cmd.hasOption("batchsize")) {
	        	batchSize = ((Number)cmd.getParsedOptionValue("batchsize")).intValue();
	        }
	        
	        if (cmd.hasOption("pctwrite")) {
	        	pctWrite = ((Number)cmd.getParsedOptionValue("pctwrite")).intValue();
	        }
	        
	        if (cmd.hasOption("time")) {
	        	benchTime = ((Number)cmd.getParsedOptionValue("time")).intValue();
	        }
	        
	        if (cmd.hasOption("host")) {
	        	host = cmd.getOptionValue("host");
	        }
	        
	        if (cmd.hasOption("port")) {
	        	port = ((Number)cmd.getParsedOptionValue("port")).intValue();
	        }
	        
	        if (cmd.hasOption("user")) {
	        	user = cmd.getOptionValue("user");
	        }
	        
	        if (cmd.hasOption("password")) {
	        	password = cmd.getOptionValue("password");
	        }
	        
	        if (cmd.hasOption("usessl")) {
	        	useSSL = true;
	        }
	        
	        if (cmd.hasOption("cacert")) {
	        	caCertPath = cmd.getOptionValue("cacert");
	        }
	        
	        if (cmd.hasOption("replfactor")) {
	        	replicationFactor = ((Number)cmd.getParsedOptionValue("replfactor")).intValue();
	        }
	        
	        if (cmd.hasOption("writeconcern")) {
	        	writeConcern = ((Number)cmd.getParsedOptionValue("writeconcern")).intValue();
	        }
        
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("arangokvload", options);
            System.exit(1);
        }
	}
	
	private static void createDatabase(ArangoDB arangoDB) {
		try {
			ArangoDatabase db = arangoDB.db(dbName);
			if (!db.exists()) {
				arangoDB.createDatabase(dbName);
				System.out.println("Database created: " + dbName);
			}
		} catch (ArangoDBException e) {
			System.err.println("Failed to create database: " + dbName + "; " + e.getMessage());
			return;
		}
	}
	
	private static void recreateCollection(ArangoDB arangoDB) {
		try {
			if (arangoDB.db(dbName).collection(collectionName).exists()) {
				arangoDB.db(dbName).collection(collectionName).drop();
			}
			CollectionEntity collEntity = arangoDB.db(dbName).createCollection(collectionName, 
					new CollectionCreateOptions()
							.replicationFactor(replicationFactor)
							.minReplicationFactor(writeConcern));
			System.out.println("Collection created: " + collEntity.getName());
		} catch (ArangoDBException e) {
			System.err.println("Failed to create collection: " + collectionName + "; " + e.getMessage());
			return;
		}
	}

	private static void populateData(ArangoDB arangoDB, int threadIndex) {
		ArangoCollection coll = arangoDB.db(dbName).collection(collectionName);
		
		long startTime, endTime = 0;
		int startIdx = (numDocs / numThreads) * threadIndex;
		int endIdx = startIdx + (numDocs / numThreads);
		if (threadIndex == numThreads - 1) {
			endIdx = numDocs;
		}
		
		startTime = System.currentTimeMillis();
		ArrayList<VPackSlice> batch = new ArrayList<VPackSlice>();
		
		try {
			for (int i = startIdx; i < endIdx; i++) {
				batch.add(buildObject(i));
				if ((i+1) % batchSize == 0) {
					coll.insertDocuments(batch);
					batch.clear();
				}
			}
		} catch (ArangoDBException e) {
			System.err.println("Failed to fill collection. " + e.getMessage());
			e.printStackTrace();
			return;
		}
		endTime = System.currentTimeMillis();
		threadStats[threadIndex].insertTime = (endTime-startTime);
	    System.out.println("Thread " + threadIndex + " execution time: " + (endTime-startTime) + "ms");
	}
	
	private static void populateDataMultithreaded(ArangoDB arangoDB) {
		ArrayList<Thread> threads = new ArrayList<Thread>();
		
		System.out.println("Inserting " + numDocs + " documents");
		
		for (int i = 0; i < numThreads; i++) {
			final int threadIndex = i;
			Thread thread = new Thread(() -> {
				populateData(arangoDB, threadIndex);
			});
			threads.add(thread);
			thread.start();
		}
		try {
			for (Thread thread : threads) {
				thread.join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static void benchReadWriteLoad(ArangoDB arangoDB, int threadIndex) {
		ArangoCollection coll = arangoDB.db(dbName).collection(collectionName);

		long startTime = System.currentTimeMillis();
		long endTime = startTime + ((long)benchTime)*1000;
		long curTime = startTime;

		ArrayList<VPackSlice> batch = new ArrayList<VPackSlice>();
		ArrayList<String> batchIds = new ArrayList<String>();

		long numReads = 0;
		long numWrites = 0;

		try {
			while (curTime < endTime) {
				if (random.nextInt(100) < pctWrite) {
					// perform a batch of write operations
					for (int i = 0; i < batchSize; i++) {
						batch.add(buildObject(random.nextInt(numDocs)));
					}
					coll.updateDocuments(batch);
					batch.clear();
					numWrites += batchSize;
				} else {
					// perform a batch of read operations
					for (int i = 0; i < batchSize; i++) {
						batchIds.add("" + random.nextInt(numDocs));
					}
					coll.getDocuments(batchIds, VPackSlice.class);
					batchIds.clear();
					numReads += batchSize;
				}
				curTime = System.currentTimeMillis();
			}
		} catch (ArangoDBException e) {
			System.err.println("Database error: " + e.getMessage());
			e.printStackTrace();
			return;
		}
		
		threadStats[threadIndex].numReads = numReads;
		threadStats[threadIndex].numWrites = numWrites;
		System.out.println("Thread " + threadIndex + " completed " + numWrites + " writes and " + numReads + " reads.");
	}

	private static void benchReadWriteLoadMultithreaded(ArangoDB arangoDB) {
		ArrayList<Thread> threads = new ArrayList<Thread>();
		
		System.out.println("Running benchmark for " + benchTime + " seconds");
		
		for (int i = 0; i < numThreads; i++) {
			final int threadIndex = i;
			Thread thread = new Thread(() -> {
				benchReadWriteLoad(arangoDB, threadIndex);
			});
			threads.add(thread);
			thread.start();
		}
		try {
			for (Thread thread : threads) {
				thread.join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private static ArangoDB connectToDB() {
		try {
			ArangoDB.Builder builder = new ArangoDB.Builder();
			builder.host(host, port)
				.user(user)
				.password(password);

			if (useSSL) {
				FileInputStream is = new FileInputStream (caCertPath);
				CertificateFactory cf = CertificateFactory.getInstance("X.509");
				X509Certificate caCert = (X509Certificate) cf.generateCertificate(is);

				TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
				KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
				ks.load(null);
				ks.setCertificateEntry("caCert", caCert);

				tmf.init(ks);

				SSLContext sslContext = SSLContext.getInstance("TLS");
				sslContext.init(null, tmf.getTrustManagers(), null);
				
				builder.useSsl(useSSL)
					.sslContext(sslContext);
			}

			ArangoDB arangoDB = builder.build();
			ArangoDBVersion version = arangoDB.getVersion();
			System.out.println("Connected to ArangoDB " + version.getVersion());
			return arangoDB;
		}
		catch (Exception e) {
			System.out.println("Error while connecting to the database " + e.getMessage());
			System.exit(1);
			return null;
		}
	}
	
	private static void printSummary() {
		long totalReads = 0;
		long totalWrites = 0;
		for (ThreadStat ts : threadStats) {
			totalReads += ts.numReads;
			totalWrites += ts.numWrites;
		}
		long insertTime = 0;
		for (ThreadStat ts : threadStats) {
			if (ts.insertTime > insertTime) {
				insertTime = ts.insertTime;
			}
		}
		VPackSlice obj = buildObject((numDocs-1) / 2);
		System.out.println("Object size " + obj.getByteSize());
		System.out.println("Total insert time: " + insertTime + "ms");
		System.out.println("Inserts per second: " + ((long)numDocs) * 1000 / insertTime);
		if (benchTime > 0) {
			System.out.println("Reads per second: " + totalReads / benchTime);
			System.out.println("Updates per second: " + totalWrites / benchTime);
			System.out.println("Total ops per second: " + (totalReads + totalWrites) / benchTime);
		}
	}

	public static void main(String[] args) {
		parseCommandLineOptions(args);
		
		ArangoDB arangoDB = connectToDB();
		
		threadStats = new ThreadStat[numThreads];
		for (int i = 0; i < numThreads; i++) {
			threadStats[i] = new ThreadStat();
		}
		
		createDatabase(arangoDB);
		recreateCollection(arangoDB);
		populateAttributeDictionary();
		populateDataMultithreaded(arangoDB);
		benchReadWriteLoadMultithreaded(arangoDB);

	    arangoDB.shutdown();
	    
	    printSummary();
	}

}
