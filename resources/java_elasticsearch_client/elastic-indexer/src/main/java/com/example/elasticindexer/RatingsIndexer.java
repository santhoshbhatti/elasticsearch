package com.example.elasticindexer;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class RatingsIndexer {

	public List<Rating> parseRatingCsv() {
		
		List<Rating> ratings=new ArrayList<>();
		try (Reader in = new FileReader("/home/santoshb/Documents/elasticsearch/movielens_dataset/ratings.csv");) {
			Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);
			//userId,movieId,rating,timestamp
			
			for (CSVRecord record : records) {
				String userId = record.get("userId");
				String movieId = record.get("movieId");
				String rating = record.get("rating");
				String timestamp = record.get("timestamp");
				Rating ratingObj =new Rating(userId, movieId, rating, timestamp);
				ratings.add(ratingObj);
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ratings;
	}
	
	public List<Tag> parseTagsCsv() {
		
		List<Tag> tags=new ArrayList<>();
		try (Reader in = new FileReader("/home/santoshb/Documents/elasticsearch/movielens_dataset/tags.csv");) {
			Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);
			//userId,movieId,tag,timestamp
			
			for (CSVRecord record : records) {
				String userId = record.get("userId");
				String movieId = record.get("movieId");
				String tag = record.get("tag");
				String timestamp = record.get("timestamp");
				Tag ratingObj =new Tag(userId, movieId, tag, timestamp);
				tags.add(ratingObj);
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tags;
	}

}
class Movie{
	String movieId;
	String title;
	String year;
	String[] genres;
	public Movie(String movieId, String title, String year, String[] genres) {
		super();
		this.movieId = movieId;
		this.title = title;
		this.year = year;
		this.genres = genres;
	}
	public String getMovieId() {
		return movieId;
	}
	public void setMovieId(String movieId) {
		this.movieId = movieId;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public String[] getGenres() {
		return genres;
	}
	public void setGenres(String[] genres) {
		this.genres = genres;
	}
	
	
}
class Rating{
	String userId;
	String movieId;
	String rating;
	String timestamp;
	String title;
	public Rating(String userId, String movieId, String rating, String timestamp) {
		super();
		this.userId = userId;
		this.movieId = movieId;
		this.rating = rating;
		this.timestamp = timestamp;
	}
	public String getMovieName() {
		return title;
	}
	public void setMovieName(String movieName) {
		this.title = movieName;
	}
	public Map toMap() {
		Map<String,String> ratingObjMap = new HashMap<>();
		ratingObjMap.put("userId", userId);
		ratingObjMap.put("movieId", movieId);
		ratingObjMap.put("rating", rating);
		ratingObjMap.put("timestamp", timestamp);
		ratingObjMap.put("title", title);
		return ratingObjMap;
	}
	
	
	
	
	
}