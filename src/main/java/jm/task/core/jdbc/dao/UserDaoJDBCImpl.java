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

    private final Logger logger = LoggerFactory.getLogger(UserDaoJDBCImpl.class);

    private final String T_CREATE = "CREATE TABLE users (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "name VARCHAR(255) NOT NULL, " +
            "lastName VARCHAR(255) NOT NULL, " +
            "age TINYINT NOT NULL" +
            ");";


    private final String U_CREATE = "INSERT INTO users (name, lastName, age) VALUES (?, ?, ?)";

    private final String T_DROP = "DROP TABLE users;";

    private final String U_REMOVE = "DELETE FROM users WHERE id = ? ";

    private final String T_ALL = "SELECT * FROM users;";

    private final String T_CLEAN = "TRUNCATE TABLE users;";

    private final Connection connection = new Util().getConnection();

    public void createUsersTable() {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(T_CREATE);
            logger.info("Таблица 'users' успешно создана!");
        } catch (SQLException e) {
            logger.error("Ошибка при создании таблицы 'users': {}", e.getMessage(), e);
        }
    }

    public void dropUsersTable() {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(T_DROP);
            logger.info("Таблица 'users' успешно удалена!");
        } catch (SQLException e) {
            logger.error("Ошибка при удалении таблица 'users': {}", e.getMessage(), e);
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(U_CREATE)) {
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
        try (PreparedStatement preparedStatement = connection.prepareStatement(U_REMOVE)) {
            preparedStatement.setLong(1, id);
                logger.info("Пользователь с id {} успешно удалён!", id);
        } catch (SQLException e) {
            logger.error("Ошибка при удалении пользователя с id = {}: {}", id, e.getMessage(), e);
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(T_ALL)) {
            while (resultSet.next()) {
                User user = new User(resultSet.getString("name"),
                                     resultSet.getString("lastName"),
                                     resultSet.getByte("age"));
                user.setId(resultSet.getLong("id"));
                users.add(user);
            }
        } catch (SQLException e) {
            logger.error("Ошибка при получении пользователей из БД! : {}", e.getMessage(), e);
        }
        users.forEach(System.out::println);
        return users;
    }


    public void cleanUsersTable() {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(T_CLEAN);
            logger.info("Все записи в таблице 'users' успешно удалены!");
        } catch (SQLException e) {
            logger.error("Ошибка при удалении записей таблицы 'users': {}", e.getMessage(), e);
        }
    }
}
