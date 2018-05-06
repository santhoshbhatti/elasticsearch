package com.example.elasticindexer;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class RatingsIndexer {

	public String parseRatingCsv() {
		String json = null;
		try (Reader in = new FileReader("/home/santoshb/Documents/elasticsearch/movielens_dataset/ratings.csv");) {
			Iterable<CSVRecord> records = CSVFormat.EXCEL.parse(in);
			//userId,movieId,rating,timestamp
			for (CSVRecord record : records) {
				String userId = record.get("userId");
				String movieId = record.get("movieId");
				String rating = record.get("rating");
				String timestamp = record.get("timestamp");
				
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return json;
	}

}
class Ratings{
	String userId;
	String movieId;
	String rating;
	String timestamp;
	public Ratings(String userId, String movieId, String rating, String timestamp) {
		super();
		this.userId = userId;
		this.movieId = movieId;
		this.rating = rating;
		this.timestamp = timestamp;
	}
	
	
}