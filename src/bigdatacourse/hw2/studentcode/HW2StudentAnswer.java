package bigdatacourse.hw2.studentcode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONObject;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import bigdatacourse.hw2.HW2API;

public class HW2StudentAnswer implements HW2API{
	
	// general consts
	public static final String		NOT_AVAILABLE_VALUE 	=		"na";

	// CQL stuff
  //TODO: add here create table and query designs
	
	// table names
	private static final String TABLE_ITEMS = "items_by_asin";
	private static final String TABLE_REVIEWS_BY_USER = "reviews_by_user";
	private static final String TABLE_REVIEWS_BY_ITEM = "reviews_by_item";

	// CQL: Create tables
	private static final String CQL_CREATE_ITEMS = 
			"CREATE TABLE IF NOT EXISTS " + TABLE_ITEMS + " (" +
				"asin text," +
				"title text," +
				"image text," +
				"categories set<text>," +
				"description text," +
				"PRIMARY KEY (asin)" +
			")";

	private static final String CQL_CREATE_REVIEWS_BY_USER = 
			"CREATE TABLE IF NOT EXISTS " + TABLE_REVIEWS_BY_USER + " (" +
				"reviewer_id text," +
				"review_time timestamp," +
				"asin text," +
				"reviewer_name text," +
				"rating int," +
				"summary text," +
				"review_text text," +
				"PRIMARY KEY ((reviewer_id), review_time, asin)" +
			") WITH CLUSTERING ORDER BY (review_time DESC, asin ASC)";

	private static final String CQL_CREATE_REVIEWS_BY_ITEM = 
			"CREATE TABLE IF NOT EXISTS " + TABLE_REVIEWS_BY_ITEM + " (" +
				"asin text," +
				"review_time timestamp," +
				"reviewer_id text," +
				"reviewer_name text," +
				"rating int," +
				"summary text," +
				"review_text text," +
				"PRIMARY KEY ((asin), review_time, reviewer_id)" +
			") WITH CLUSTERING ORDER BY (review_time DESC, reviewer_id ASC)";

	// CQL: Insert statements
	private static final String CQL_INSERT_ITEM = 
			"INSERT INTO " + TABLE_ITEMS + " (asin, title, image, categories, description) VALUES (?, ?, ?, ?, ?)";

	private static final String CQL_INSERT_REVIEW_BY_USER = 
			"INSERT INTO " + TABLE_REVIEWS_BY_USER + " (reviewer_id, review_time, asin, reviewer_name, rating, summary, review_text) VALUES (?, ?, ?, ?, ?, ?, ?)";

	private static final String CQL_INSERT_REVIEW_BY_ITEM = 
			"INSERT INTO " + TABLE_REVIEWS_BY_ITEM + " (asin, review_time, reviewer_id, reviewer_name, rating, summary, review_text) VALUES (?, ?, ?, ?, ?, ?, ?)";

	// CQL: Select statements
	private static final String CQL_SELECT_ITEM = 
			"SELECT * FROM " + TABLE_ITEMS + " WHERE asin = ?";

	private static final String CQL_SELECT_REVIEWS_BY_USER = 
			"SELECT * FROM " + TABLE_REVIEWS_BY_USER + " WHERE reviewer_id = ?";

	private static final String CQL_SELECT_REVIEWS_BY_ITEM = 
			"SELECT * FROM " + TABLE_REVIEWS_BY_ITEM + " WHERE asin = ?";

	// cassandra session
	private CqlSession session;
	
	// prepared statements
	private PreparedStatement pstmtInsertItem;
	private PreparedStatement pstmtInsertReviewByUser;
	private PreparedStatement pstmtInsertReviewByItem;
	private PreparedStatement pstmtSelectItem;
	private PreparedStatement pstmtSelectReviewsByUser;
	private PreparedStatement pstmtSelectReviewsByItem;
	
	
	@Override
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		if (session != null) {
			System.out.println("ERROR - cassandra is already connected");
			return;
		}
		
		System.out.println("Initializing connection to Cassandra...");
		
		this.session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
				.build();
		
		System.out.println("Initializing connection to Cassandra... Done");
	}


	@Override
	public void close() {
		if (session == null) {
			System.out.println("Cassandra connection is already closed");
			return;
		}
		
		System.out.println("Closing Cassandra connection...");
		session.close();
		System.out.println("Closing Cassandra connection... Done");
	}

	
	
	@Override
	public void createTables() {
		System.out.println("Creating tables...");
		
		session.execute(CQL_CREATE_ITEMS);
		System.out.println("Created table: " + TABLE_ITEMS);
		
		session.execute(CQL_CREATE_REVIEWS_BY_USER);
		System.out.println("Created table: " + TABLE_REVIEWS_BY_USER);
		
		session.execute(CQL_CREATE_REVIEWS_BY_ITEM);
		System.out.println("Created table: " + TABLE_REVIEWS_BY_ITEM);
		
		System.out.println("Creating tables... Done");
	}

	@Override
	public void initialize() {
		System.out.println("Preparing statements...");
		
		pstmtInsertItem = session.prepare(CQL_INSERT_ITEM);
		pstmtInsertReviewByUser = session.prepare(CQL_INSERT_REVIEW_BY_USER);
		pstmtInsertReviewByItem = session.prepare(CQL_INSERT_REVIEW_BY_ITEM);
		pstmtSelectItem = session.prepare(CQL_SELECT_ITEM);
		pstmtSelectReviewsByUser = session.prepare(CQL_SELECT_REVIEWS_BY_USER);
		pstmtSelectReviewsByItem = session.prepare(CQL_SELECT_REVIEWS_BY_ITEM);
		
		System.out.println("Preparing statements... Done");
	}

	@Override
	public void loadItems(String pathItemsFile) throws Exception {
		System.out.println("Loading items...");
		
		int maxThreads = 32;
		ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
		
		int count = 0;
		try (BufferedReader reader = new BufferedReader(new FileReader(pathItemsFile))) {
			String line;
			while ((line = reader.readLine()) != null) {
				JSONObject json = new JSONObject(line);
				
				String asin = json.getString("asin");
				String title = json.optString("title", NOT_AVAILABLE_VALUE);
				String image = json.optString("imUrl", NOT_AVAILABLE_VALUE);
				String description = json.optString("description", NOT_AVAILABLE_VALUE);
				
				// Flatten nested categories array: [[cat1, cat2], [cat3]] -> {cat1, cat2, cat3}
				Set<String> categories = new HashSet<>();
				if (json.has("categories")) {
					JSONArray outerCategories = json.getJSONArray("categories");
					for (int i = 0; i < outerCategories.length(); i++) {
						JSONArray innerCategories = outerCategories.getJSONArray(i);
						for (int j = 0; j < innerCategories.length(); j++) {
							categories.add(innerCategories.getString(j));
						}
					}
				}
				
				BoundStatement bstmt = pstmtInsertItem.bind()
						.setString(0, asin)
						.setString(1, title)
						.setString(2, image)
						.setSet(3, categories, String.class)
						.setString(4, description);
				
				executor.execute(() -> session.execute(bstmt));
				count++;
				
				if (count % 10000 == 0) {
					System.out.println("Loaded " + count + " items...");
				}
			}
		}
		
		executor.shutdown();
		executor.awaitTermination(1, TimeUnit.HOURS);
		
		System.out.println("Loading items... Done. Total: " + count);
	}

	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		System.out.println("Loading reviews...");
		
		int maxThreads = 200;
		
		// Putting the reviews in two passes to avoid too many items in the thread queue.
		// (When tried to do both together, our program crashed due to too many 
		// items in the thread queue.)

		// First pass: insert into reviews_by_user
		System.out.println("Pass 1: Loading into reviews_by_user...");
		ExecutorService executor1 = Executors.newFixedThreadPool(maxThreads);
		int count = 0;
		try (BufferedReader reader = new BufferedReader(new FileReader(pathReviewsFile))) {
			String line;
			while ((line = reader.readLine()) != null) {
				JSONObject json = new JSONObject(line);
				
				String reviewerId = json.getString("reviewerID");
				String asin = json.getString("asin");
				String reviewerName = json.optString("reviewerName", NOT_AVAILABLE_VALUE);
				int rating = (int) json.getDouble("overall");
				String summary = json.optString("summary", NOT_AVAILABLE_VALUE);
				String reviewText = json.optString("reviewText", NOT_AVAILABLE_VALUE);
				Instant reviewTime = Instant.ofEpochSecond(json.getLong("unixReviewTime"));
				
				BoundStatement bstmt = pstmtInsertReviewByUser.bind()
						.setString(0, reviewerId)
						.setInstant(1, reviewTime)
						.setString(2, asin)
						.setString(3, reviewerName)
						.setInt(4, rating)
						.setString(5, summary)
						.setString(6, reviewText);
				
				executor1.execute(() -> session.execute(bstmt));
				count++;
				
				if (count % 10000 == 0) {
					System.out.println("Pass 1: Loaded " + count + " reviews...");
				}
			}
		}
		executor1.shutdown();
		executor1.awaitTermination(1, TimeUnit.HOURS);
		System.out.println("Pass 1: Done. Total: " + count);
		
		// Second pass: insert into reviews_by_item
		System.out.println("Pass 2: Loading into reviews_by_item...");
		ExecutorService executor2 = Executors.newFixedThreadPool(maxThreads);
		count = 0;
		try (BufferedReader reader = new BufferedReader(new FileReader(pathReviewsFile))) {
			String line;
			while ((line = reader.readLine()) != null) {
				JSONObject json = new JSONObject(line);
				
				String reviewerId = json.getString("reviewerID");
				String asin = json.getString("asin");
				String reviewerName = json.optString("reviewerName", NOT_AVAILABLE_VALUE);
				int rating = (int) json.getDouble("overall");
				String summary = json.optString("summary", NOT_AVAILABLE_VALUE);
				String reviewText = json.optString("reviewText", NOT_AVAILABLE_VALUE);
				Instant reviewTime = Instant.ofEpochSecond(json.getLong("unixReviewTime"));
				
				BoundStatement bstmt = pstmtInsertReviewByItem.bind()
						.setString(0, asin)
						.setInstant(1, reviewTime)
						.setString(2, reviewerId)
						.setString(3, reviewerName)
						.setInt(4, rating)
						.setString(5, summary)
						.setString(6, reviewText);
				
				executor2.execute(() -> session.execute(bstmt));
				count++;
				
				if (count % 10000 == 0) {
					System.out.println("Pass 2: Loaded " + count + " reviews...");
				}
			}
		}
		executor2.shutdown();
		executor2.awaitTermination(1, TimeUnit.HOURS);
		System.out.println("Pass 2: Done. Total: " + count);
		
		System.out.println("Loading reviews... Done.");
	}

	@Override
	public String item(String asin) {
		// you should return the item's description based on the formatItem function.
    // if it does not exist, return the string "not exists"
    
		BoundStatement bstmt = pstmtSelectItem.bind().setString(0, asin);
		ResultSet rs = session.execute(bstmt);
		Row row = rs.one();
		
		if (row == null) {
			return "not exists";
		}
		
		String title = row.getString("title");
		String image = row.getString("image");
		Set<String> categoriesSet = row.getSet("categories", String.class);
		String description = row.getString("description");
		
		// Convert to TreeSet for sorted output
		TreeSet<String> categories = new TreeSet<>(categoriesSet);
		
		return formatItem(asin, title, image, categories, description);
	}
	
	
	@Override
	public Iterable<String> userReviews(String reviewerID) {
		// the order of the reviews should be by the time (desc), then by the asin
		BoundStatement bstmt = pstmtSelectReviewsByUser.bind().setString(0, reviewerID);
		ResultSet rs = session.execute(bstmt);
		
		ArrayList<String> reviewReprs = new ArrayList<>();
		int count = 0;
		
		for (Row row : rs) {
			Instant reviewTime = row.getInstant("review_time");
			String asin = row.getString("asin");
			String reviewerName = row.getString("reviewer_name");
			int rating = row.getInt("rating");
			String summary = row.getString("summary");
			String reviewText = row.getString("review_text");
			
			reviewReprs.add(formatReview(reviewTime, asin, reviewerID, reviewerName, rating, summary, reviewText));
			count++;
		}
		
		System.out.println("total reviews: " + count);
		return reviewReprs;
	}

	@Override
	public Iterable<String> itemReviews(String asin) {
		// the order of the reviews should be by the time (desc), then by the reviewerID
		BoundStatement bstmt = pstmtSelectReviewsByItem.bind().setString(0, asin);
		ResultSet rs = session.execute(bstmt);
		
		ArrayList<String> reviewReprs = new ArrayList<>();
		int count = 0;
		
		for (Row row : rs) {
			Instant reviewTime = row.getInstant("review_time");
			String reviewerId = row.getString("reviewer_id");
			String reviewerName = row.getString("reviewer_name");
			int rating = row.getInt("rating");
			String summary = row.getString("summary");
			String reviewText = row.getString("review_text");
			
			reviewReprs.add(formatReview(reviewTime, asin, reviewerId, reviewerName, rating, summary, reviewText));
			count++;
		}
		
		System.out.println("total reviews: " + count);
		return reviewReprs;
	}

	
	
	// Formatting methods, do not change!
	private String formatItem(String asin, String title, String imageUrl, Set<String> categories, String description) {
		String itemDesc = "";
		itemDesc += "asin: " + asin + "\n";
		itemDesc += "title: " + title + "\n";
		itemDesc += "image: " + imageUrl + "\n";
		itemDesc += "categories: " + categories.toString() + "\n";
		itemDesc += "description: " + description + "\n";
		return itemDesc;
	}

	private String formatReview(Instant time, String asin, String reviewerId, String reviewerName, Integer rating, String summary, String reviewText) {
		String reviewDesc = 
			"time: " + time + 
			", asin: " 	+ asin 	+
			", reviewerID: " 	+ reviewerId +
			", reviewerName: " 	+ reviewerName 	+
			", rating: " 		+ rating	+ 
			", summary: " 		+ summary +
			", reviewText: " 	+ reviewText + "\n";
		return reviewDesc;
	}

}
