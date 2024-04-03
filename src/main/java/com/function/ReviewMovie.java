package com.function;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.Optional;

public class ReviewMovie {
    private static final String DB_CONNECTION_STRING = System.getenv("DB_CONNECTION_STRING");

    @FunctionName("reviewFunction")
    public HttpResponseMessage run(
        @HttpTrigger(name = "req", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
        final ExecutionContext context) {

            String reviewData = request.getBody().orElse(null);

            if (reviewData == null) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass movie data in the request body for POST requests.").build();
            }

            try {
                insertReviewIntoDatabase(reviewData, context);
                return request.createResponseBuilder(HttpStatus.OK).body("Review saved successfully.").build();
            } catch (Exception e) {
                context.getLogger().severe("Exception occurred while saving movie: " + e.toString());
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to save movie: " + e.getMessage()).build();
            }
        
        }

    private void insertReviewIntoDatabase(String reviewData, final ExecutionContext context) throws SQLException, ClassNotFoundException {
        String[] data = reviewData.split(",");
        
        Class.forName("org.postgresql.Driver");

        try (Connection connection = DriverManager.getConnection(DB_CONNECTION_STRING)) {
            String sql = "INSERT INTO reviews (movie_id, title, opinion, rating, review_date, reviewer) VALUES ((SELECT id FROM movies WHERE title = ?), ?, ?, ?, TO_TIMESTAMP(?, 'YYYY-MM-DD'), ?)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, data[0]);
                statement.setString(2, data[0]);
                statement.setString(3, data[1]);
                statement.setInt(4, Integer.parseInt(data[2]));
                statement.setString(5, data[3]);
                statement.setString(6, data[4]);
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            context.getLogger().severe("SQL Exception: " + e.getMessage());
            throw e;
        }
    }

}