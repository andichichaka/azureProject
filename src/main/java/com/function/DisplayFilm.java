package com.function;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import java.sql.ResultSet;
import com.google.gson.Gson;

public class DisplayFilm {
    private static final String DB_CONNECTION_STRING = System.getenv("DB_CONNECTION_STRING");

    @FunctionName("displayFunction")
    public HttpResponseMessage run(
        @HttpTrigger(name = "req", methods = {HttpMethod.GET}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
        final ExecutionContext context) {

            String movieName = request.getQueryParameters().get("title");
         try{
            if (movieName == null) {
                String movies = getMovies();
                String reviews = getReviews();
                return request.createResponseBuilder(HttpStatus.OK).body(movies + reviews).build();
            }
        }catch (Exception e) {
            context.getLogger().severe("Exception occurred while fetching movie: " + e.toString());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to fetch movie: " + e.getMessage()).build();
        }

            try {
                String movieJson = getMovieFromDatabase(movieName, context);
                String reviewJson = getReviewsFromDB(movieName, context);
                return request.createResponseBuilder(HttpStatus.OK).body(movieJson + reviewJson).build();
            } catch (Exception e) {
                context.getLogger().severe("Exception occurred while fetching movie: " + e.toString());
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to fetch movie: " + e.getMessage()).build();
            }
        }

    private String getMovieFromDatabase(String movieTitle, final ExecutionContext context) throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        try (Connection connection = DriverManager.getConnection(DB_CONNECTION_STRING)) {
            String sql = "SELECT title, year, genre, description, director, actors FROM movies WHERE title = ?";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, movieTitle);
                
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        Map<String, Object> movie = new HashMap<>();
                        movie.put("title", resultSet.getString("title"));
                        movie.put("year", resultSet.getInt("year"));
                        movie.put("genre", resultSet.getString("genre"));
                        movie.put("description", resultSet.getString("description"));
                        movie.put("director", resultSet.getString("director"));
                        movie.put("actors", resultSet.getString("actors"));
                        
                        Gson gson = new Gson();
                        return gson.toJson(movie);
                    } else {
                        return "{}"; 
                    }
                }
            }
        } catch (SQLException e) {
            context.getLogger().severe("SQL Exception: " + e.getMessage());
            throw e;
        }
    }

    private String getReviewsFromDB(String title, ExecutionContext context) throws SQLException, ClassNotFoundException{
        Class.forName("org.postgresql.Driver");

        try(Connection connection = DriverManager.getConnection(DB_CONNECTION_STRING)){
            String sql = "SELECT title, opinion, rating, review_date, reviewer FROM reviews WHERE title = ?";
            try(PreparedStatement statement = connection.prepareStatement(sql)){
                statement.setString(1, title);
                try(ResultSet resultSet = statement.executeQuery()){
                    if (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();

                    map.put("title", resultSet.getString("title"));
                    map.put("opinion", resultSet.getString("opinion"));
                    map.put("rating", resultSet.getInt("rating"));
                    map.put("review_date", resultSet.getString("review_date"));
                    map.put("reviewer", resultSet.getString("reviewer"));

                    Gson gson = new Gson();
                    return gson.toJson(map);
                    }else{
                        return "{}";
                }
            }
        }
    }catch(SQLException e){
        context.getLogger().severe("SQL Exception: " + e.getMessage());
        throw e;
        }
    }

    private String getMovies() throws SQLException, ClassNotFoundException {
    Class.forName("org.postgresql.Driver");
    List<Map<String, Object>> moviesList = new ArrayList<>();

    try (Connection connection = DriverManager.getConnection(DB_CONNECTION_STRING)) {
        String sql = "SELECT title, year, genre, description, director, actors FROM movies";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet rs = statement.executeQuery();

            // Iterate through the result set and collect data
            while (rs.next()) {
                Map<String, Object> movie = new HashMap<>();
                movie.put("title", rs.getString("title"));
                movie.put("year", rs.getInt("year"));
                movie.put("genre", rs.getString("genre"));
                movie.put("description", rs.getString("description"));
                movie.put("director", rs.getString("director"));
                movie.put("actors", rs.getString("actors"));
                moviesList.add(movie);
            }
        }
    }

    Gson gson = new Gson();
    return gson.toJson(moviesList);
}

    private String getReviews() throws SQLException, ClassNotFoundException{
        Class.forName("org.postgresql.Driver");
        List<Map<String, Object>> reviews = new ArrayList<>();

        try(Connection connection = DriverManager.getConnection(DB_CONNECTION_STRING)){
            String sql = "SELECT title, opinion, rating, review_date, reviewer FROM reviews";
            try(PreparedStatement statement = connection.prepareStatement(sql)){
                ResultSet resultSet = statement.executeQuery();

                while(resultSet.next()){
                    Map<String, Object> rev = new HashMap<>();
                    rev.put("title", resultSet.getString("title"));
                    rev.put("opinion", resultSet.getString("opinion"));
                    rev.put("rating", resultSet.getInt("rating"));
                    rev.put("review_date", resultSet.getString("review_date"));
                    rev.put("reviewer", resultSet.getString("reviewer"));
                    reviews.add(rev); 
                }
            }
        }
        Gson gson = new Gson();
        return gson.toJson(reviews);
    }
}