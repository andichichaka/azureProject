package com.function;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.Optional;

public class InsertMovie {
    private static final String DB_CONNECTION_STRING = System.getenv("DB_CONNECTION_STRING");

    @FunctionName("movieFunction")
    public HttpResponseMessage run(
        @HttpTrigger(name = "req", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
        final ExecutionContext context) {

            String movieData = request.getBody().orElse(null);

            if (movieData == null) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass movie data in the request body for POST requests.").build();
            }

            try {
                insertMovieIntoDatabase(movieData, context);
                return request.createResponseBuilder(HttpStatus.OK).body("Movie saved successfully.").build();
            } catch (Exception e) {
                context.getLogger().severe("Exception occurred while saving movie: " + e.toString());
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to save movie: " + e.getMessage()).build();
            }
        
}

    private void insertMovieIntoDatabase(String movieData, final ExecutionContext context) throws SQLException, ClassNotFoundException {
        String[] data = movieData.split(",");
        
        Class.forName("org.postgresql.Driver");

        try (Connection connection = DriverManager.getConnection(DB_CONNECTION_STRING)) {
            String sql = "INSERT INTO movies (title, year, genre, description, director, actors) VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (int i = 0; i < data.length; i++) {
                    statement.setString(i + 1, data[i]);
                }
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            context.getLogger().severe("SQL Exception: " + e.getMessage());
            throw e;
        }
    }

}