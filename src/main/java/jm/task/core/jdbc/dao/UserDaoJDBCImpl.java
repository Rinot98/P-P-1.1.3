package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    private static final Logger logger = LoggerFactory.getLogger(UserDaoJDBCImpl.class);

    private static final String TABLE_CREATE_SQL = "CREATE TABLE users (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "name VARCHAR(255) NOT NULL, " +
            "lastName VARCHAR(255) NOT NULL, " +
            "age TINYINT NOT NULL" +
            ");";


    private static final String USER_CREATE_SQL = "INSERT INTO users (name, lastName, age) VALUES (?, ?, ?)";

    private static final String TABLE_DROP_SQL = "DROP TABLE users;";

    private static final String USER_REMOVE_SQL = "DELETE FROM users WHERE id = ? ";

    private static final String TABLE_GET_ALL_SQL = "SELECT * FROM users;";

    private static final String TABLE_CLEAN_SQL = "TRUNCATE TABLE users;";

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        try (Connection connection = Util.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate(TABLE_CREATE_SQL);
            logger.info("Таблица 'users' успешно создана!");
        } catch (SQLException e) {
            logger.error("Ошибка при создании таблицы 'users': {}", e.getMessage(), e);
        }
    }

    public void dropUsersTable() {
        try (Connection connection = Util.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate(TABLE_DROP_SQL);
            logger.info("Таблица 'users' успешно удалена!");
        } catch (SQLException e) {
            logger.error("Ошибка при удалении таблица 'users': {}", e.getMessage(), e);
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (Connection connection = Util.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(USER_CREATE_SQL)) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();
            logger.info("Пользователь {} {} успешно создан!", name, lastName);
        } catch (SQLException e) {
            logger.error("Ошибка при создании пользователя {} {}: {}", name, lastName, e.getMessage(), e);
        }
    }

    public void removeUserById(long id) {
        try (Connection connection = Util.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(USER_REMOVE_SQL)) {
            preparedStatement.setLong(1, id);
            int effect = preparedStatement.executeUpdate();
            if (effect > 0) {
                logger.info("Пользователь с id {} успешно удалён!", id);
            } else {
                logger.warn("Пользователь с id {} не найден :'( !", id);
            }
        } catch (SQLException e) {
            logger.error("Ошибка при удалении пользователя с id = {}: {}", id, e.getMessage(), e);
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(TABLE_GET_ALL_SQL)) {
            while (resultSet.next()) {
                Long userId = resultSet.getLong(1);
                String userName = resultSet.getString(2);
                String userLastName = resultSet.getString(3);
                Byte userAge = resultSet.getByte(4);

                User user = new User(userName, userLastName, userAge);
                user.setId(userId);
                users.add(user);
            }
            if (!users.isEmpty()) {
                logger.info("Удалось получить {} пользователей из БД.", users.size());
            } else {
                logger.info("Данные получить не удалось, данная БД пуста, пользователей 0");
            }
        } catch (SQLException e) {
            logger.error("Ошибка при получении пользователей из БД! : {}", e.getMessage(), e);
        }
        users.forEach(System.out::println);
        return users;
    }

    public void cleanUsersTable() {
        try (Connection connection = Util.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate(TABLE_CLEAN_SQL);
            logger.info("Все записи в таблице 'users' успешно удалены!");
        } catch (SQLException e) {
            logger.error("Ошибка при удалении записей таблицы 'users': {}", e.getMessage(), e);
        }
    }
}
