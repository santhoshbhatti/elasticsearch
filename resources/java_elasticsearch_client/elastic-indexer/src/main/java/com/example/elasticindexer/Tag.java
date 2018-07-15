package com.example.elasticindexer;

import java.util.HashMap;
import java.util.Map;

public class Tag {

	String userId;
	String movieId;
	String tag;
	String timestamp;
	private String movieName;

	public Tag(String userId, String movieId, String tag, String timestamp) {
		super();
		this.userId = userId;
		this.movieId = movieId;
		this.tag = tag;
		this.timestamp = timestamp;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getMovieId() {
		return movieId;
	}

	public void setMovieId(String movieId) {
		this.movieId = movieId;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String setMovieName(String title) {
		// TODO Auto-generated method stub
		return this.movieName = title;
	}

	public Map toMap() {
		Map<String, String> ratingObjMap = new HashMap<>();
		ratingObjMap.put("userId", userId);
		ratingObjMap.put("movieId", movieId);
		ratingObjMap.put("tag", tag);
		ratingObjMap.put("timestamp", timestamp);
		ratingObjMap.put("title", movieName);
		return ratingObjMap;
	}

}
