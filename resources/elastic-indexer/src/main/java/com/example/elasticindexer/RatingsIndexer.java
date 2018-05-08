package com.example.elasticindexer;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.Header;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
@Service
public class RatingsIndexer {

	@Autowired
	RestHighLevelClient client;
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
			Header headers=null;
			BulkRequest bulkRequest=new BulkRequest();
			//DocWriteRequest request=new Do;
			bulkRequest.add(request);
			client.bulk(bulkRequest, headers);
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