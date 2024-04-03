package com.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;

public class AvgRating {
    private static final String DB_CONNECTION_STRING = System.getenv("DB_CONNECTION_STRING");

    @FunctionName("avgRating")
    public void run(
        @TimerTrigger(name = "timerInfo", schedule = "0 30 11 * * *") String timerInfo, final ExecutionContext context) {
        context.getLogger().info("Java Timer trigger function executed at: " + java.time.LocalDateTime.now());

        try {
            insertRatingIntoDatabase(context);
        } catch (Exception e) {
            context.getLogger().severe("Exception occurred while fetching average rating: " + e.toString());
        }
    }

    private void insertRatingIntoDatabase(final ExecutionContext context) throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
    
        try (Connection connection = DriverManager.getConnection(DB_CONNECTION_STRING)) {
            String sql = "UPDATE movies SET average_rating = (SELECT AVG(rating) FROM reviews WHERE movie_id = movies.id GROUP BY movie_id)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int affectedRows = statement.executeUpdate();
                context.getLogger().info(affectedRows + " rows updated with average rating.");
            }
        } catch (SQLException e) {
            context.getLogger().severe("SQL Exception: " + e.getMessage());
            throw e;
        }
    }
}
