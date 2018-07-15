package com.example.elasticindexer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.RequestEntity.HeadersBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class ElasticIndexerApplication {
	private class Pair{
		public String one;
		public String two;
		public Pair(String one,String two){
			this.one=one;
			this.two=two;
		}
	}
	private static final String CREATE_META_DATA="{ \"create\" : { \"_index\" : \"movies\", \"_type\" : \"movie\", \"_id\" : \"<movie_id>\"} }";

	@Autowired
	RestHighLevelClient client;

	@Autowired
	RatingsIndexer ratingsIndexer;
	
	@GetMapping("/hello")
	public String hello() throws IOException {
		/*IndexRequest request = new IndexRequest("movies", "movie", "100");
		String jsonString = "{\"genre\": [\"IMAX\",\"Sci-Fi\"],\"title\": \"Interstellar\",\"year\": 2014 }";
		request.source(jsonString, XContentType.JSON);
		IndexResponse response = client.index(request);
		List<String> moviejsons = readAndTransformCsvMovies();
		System.out.println(moviejsons);*/
		
		List<Rating> ratings=ratingsIndexer.parseRatingCsv();
		Map<String,Movie> map=getIdToMovieMapping();
		ratings.forEach(rating -> rating.setMovieName(map.get(rating.movieId).getTitle()));
		BulkRequest bulkRequest=new BulkRequest();
		ratings.forEach((rating) ->{
			IndexRequest request = new IndexRequest("ratings", "rating", UUID.randomUUID().toString());
			request.source(rating.toMap());
			bulkRequest.add(request);
			
			
		});
		
		try {
			Header headers= new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json");
			client.bulk(bulkRequest, headers);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return "Hello!!!!!!!" + (client != null) ;
	}
	
	@GetMapping("/tags")
	public String tagIndex() throws IOException {
		
		List<Tag> tags=ratingsIndexer.parseTagsCsv();
		Map<String,Movie> map=getIdToMovieMapping();
		tags.forEach(tag -> tag.setMovieName(map.get(tag.movieId).getTitle()));
		BulkRequest bulkRequest=new BulkRequest();
		tags.forEach((tag) ->{
			IndexRequest request = new IndexRequest("tags", "tag", UUID.randomUUID().toString());
			request.source(tag.toMap());
			bulkRequest.add(request);
			
			
		});
		
		try {
			Header headers= new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json");
			client.bulk(bulkRequest, headers);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return "Hello!!!!!!!" + (client != null) ;
	}
	
	public Map<String,Movie> getIdToMovieMapping(){
		Map<String,Movie> movieMap=new HashMap<>();
		try (FileReader fr = new FileReader("/home/santoshb/Documents/elasticsearch/movielens_dataset/movies.csv");
				BufferedReader br = new BufferedReader(fr)) {

			String line = null;
			br.readLine();
			while ((line = br.readLine()) != null) {
				//System.out.println(line);
				// 11,"American President, The (1995)",Comedy|Drama|Romance
				Movie movie = parseMovieJsonToMovie(line);
				movieMap.put(movie.getMovieId(), movie);

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return movieMap;
		
	}

	private Movie parseMovieJsonToMovie(String line) {
		String movieId = line.substring(0,line.indexOf(","));
		String title=getTitle(line);
		//System.out.print(title);
		String year=getYear(line);
		//System.out.print(","+year);
		String[] genres=getGeneres(line);
		
		Movie movie =new Movie(movieId, title, year, genres);
		return movie;
	}

	public  List<String> readAndTransformCsvMovies() throws IOException {
		List<String> movieJsons = new ArrayList<>();
		// movieId,title(year),genres|genre

		try (FileReader fr = new FileReader("/home/santoshb/Documents/elasticsearch/movielens_dataset/movies.csv");
				BufferedReader br = new BufferedReader(fr)) {

			String line = null;
			br.readLine();
			while ((line = br.readLine()) != null) {
				//System.out.println(line);
				// 11,"American President, The (1995)",Comedy|Drama|Romance
				Pair pair = parseMovieJson(line);
				movieJsons.add(pair.one);movieJsons.add(pair.two);

			}
		}

		return movieJsons;
	}

	private  Pair parseMovieJson(String line) {
		String movieId = line.substring(0,line.indexOf(","));
		String title=getTitle(line);
		//System.out.print(title);
		String year=getYear(line);
		//System.out.print(","+year);
		String[] genres=getGeneres(line);
		//System.out.println(","+Arrays.toString(genres));
		
		// String movieid=line.substring(0,line.indexOf(","));
		/*if (line.contains("\"")) {
			line = line.replaceAll("\"", "");
		}
		boolean genreFlag = line.indexOf("(no genres listed)") >= 0;
		if (genreFlag) {
			line = line.substring(0, line.indexOf("(no genres listed)") - 1);
		}
		String titleDate = line.substring(line.indexOf(",") + 1, line.lastIndexOf(","));
		int titleEndIndex=titleDate.lastIndexOf("(")<0?
		String title = titleDate.substring(0, titleDate.lastIndexOf("("));

		String year = titleDate.substring(titleDate.lastIndexOf("(") + 1, titleDate.lastIndexOf(")"));
		String[] genres = null;
		if (!genreFlag) {
			String genreStr = line.substring(line.lastIndexOf(")") + 2);

			if (!genreStr.startsWith("(")) {
				genres = line.substring(line.lastIndexOf(")") + 2).split("\\|");
			}
		}*/
		
		
		String metaData=CREATE_META_DATA.replace("<movie_id>", movieId);
		
		StringBuilder builder = new StringBuilder("{");
		builder.append("\"id\":\"").append(movieId).append("\",");
		builder.append("\"title\":");
		builder.append("\"").append(title).append("\"");
		if(year!=null){
		builder.append(",\"year\":");
		builder.append("\"" + year + "\"");
		}
		if (genres != null && genres.length > 0) {
			builder.append(",\"genre\":");
			String genre = Arrays.stream(genres).map((s) -> "\"" + s + "\"").collect(Collectors.joining(","));
			builder.append("[" + genre + "]");
		}
		builder.append("}");
		
		return new Pair(metaData,builder.toString());
	}

	private static String[] getGeneres(String line) {
		String[] generes=null;
		String genere=line.substring(line.lastIndexOf(",")+1);
		if(!genere.contains("(")){
			generes= genere.split("\\|");
		}
		return generes;
	}

	private static String getYear(String line) {
		String title=null;
		if(line.contains("\""))
		{
			//title with date and no double quotes
			title=line.substring(line.indexOf("\"")+1,line.lastIndexOf("\""));
			
		}else{
			//check if year is present????
			
			title=line.substring(line.indexOf(",")+1, line.lastIndexOf(","));
		}
		String year=null;
		if(title.contains("(")){
			year=title.substring(title.indexOf("(")+1,title.indexOf(")"));
		}
		return year;
	}

	private static String getTitle(String line) {
		String title=null;
		if(line.contains("\""))
		{
			//title with date and no double quotes
			title=line.substring(line.indexOf("\"")+1,line.lastIndexOf("\""));
			
		}else{
			//check if year is present????
			
			title=line.substring(line.indexOf(",")+1, line.lastIndexOf(","));
		}
		
		if(title.contains("(")){
			title=title.substring(0,title.indexOf("("));
		}
		return title.trim();
	}

	@Bean
	public RestHighLevelClient elasticClient() {
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("127.0.0.1", 9200, "http")));
		return client;
	}
	
	@Bean
	public RatingsIndexer getRatingsIndexer(){
		return new RatingsIndexer();
	}

	public static void main(String[] args) {
		
		/*ElasticIndexerApplication indexer=new ElasticIndexerApplication();
		String line="162376,Stranger Things,Drama";
		Pair pair=indexer.parseMovieJson(line);
		System.out.println(pair.one);
		System.out.println(pair.two);

		
		 * StringBuilder builder1 =
		 * parseMovieJson("11,\"American President, The (1995)\",Comedy|Drama|Romance"
		 * ); System.out.println(builder1); StringBuilder builder2 =
		 * parseMovieJson("61289,Sukiyaki Western Django (2008),Action|Western"
		 * ); System.out.println(builder2); StringBuilder builder3 =
		 * parseMovieJson("83829,Scorpio Rising (1964),(no genres listed)");
		 * System.out.println(builder3);
		 
		File fos=new File("/home/santoshb/Documents/elasticsearch/movielens_dataset/movies-small.json");
		try(
				FileWriter fw=new FileWriter(fos);
				BufferedWriter bw=new BufferedWriter(fw);) {
			//ElasticIndexerApplication indexer=new ElasticIndexerApplication();
			List<String> jsons= indexer.readAndTransformCsvMovies();
			jsons.forEach(System.out::println);
			
			jsons.forEach(json -> {try {
				bw.write(json);
				bw.newLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}});
		} catch (IOException e) { // TODO
			e.printStackTrace();
		}*/

		SpringApplication.run(ElasticIndexerApplication.class, args);
	}
}
