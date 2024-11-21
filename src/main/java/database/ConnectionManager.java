package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionManager {

    private static Connection connection;

    private ConnectionManager() {

    }

    public static Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            String url = PropertiesUtils.getProperty("db.url");
            String user = PropertiesUtils.getProperty("db.username");
            String password = PropertiesUtils.getProperty("db.password");
            connection = DriverManager.getConnection(url, user, password);
        }
        return connection;
    }
}
