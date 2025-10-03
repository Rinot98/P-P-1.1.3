package jm.task.core.jdbc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

    private final static String URL = "jdbc:mysql://localhost:3306/mydatabase";
    private final static String USERNAME = "root";
    private final static String PASSWORD = "root";

    private Connection connection;
    Logger logger = LoggerFactory.getLogger(Util.class);

    public Util() {

    }

    public Connection getConnection()  {
        try {
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (SQLException e) {
            logger.error("Smth wrong {}", e.getMessage(), e);
        }
        return connection;
    }
}
