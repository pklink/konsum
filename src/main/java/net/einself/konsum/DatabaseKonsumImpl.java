package net.einself.konsum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

public class DatabaseKonsumImpl implements DatabaseKonsum {

    private final static Logger LOGGER = LoggerFactory.getLogger(DatabaseKonsumImpl.class);

    private final DataSource dataSource;

    public DatabaseKonsumImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private boolean isOpen(Connection connection) {
        try {
            final var closed = connection.isClosed();
            LOGGER.debug("Is connection #{} closed? {}", connection.hashCode(), closed);
            return !closed;
        } catch (SQLException throwables) {
            LOGGER.error("Cannot check if connection is closed", throwables);
            return false;
        }
    }

    public void connection(Consumer<Connection> consumer) {
        final Connection connection;

        try {
            connection = dataSource.getConnection();
            LOGGER.debug("Retrieved connection #{}", connection.hashCode());
        } catch (SQLException throwables) {
            LOGGER.error("Cannot retrieve connection", throwables);
            throw new RuntimeException(throwables);
        }

        LOGGER.debug("Call connection consumer #{}", connection.hashCode());
        consumer.accept(connection);

        if (isOpen(connection)) {
            try {
                connection.close();
                LOGGER.debug("Closed connection #{}", connection.hashCode());
            } catch (SQLException throwables) {
                LOGGER.error("Cannot close connection", throwables);
            }
        }
    }

    public void read(Consumer<Statement> consumer) {
        LOGGER.debug("Call read consumer #{}", consumer.hashCode());
        statement(consumer);
    }

    synchronized public void write(Consumer<Statement> consumer) {
        LOGGER.debug("Call write consumer #{}", consumer.hashCode());
        statement(consumer);
    }

    private void statement(Consumer<Statement> consumer) {
        connection(connection -> {
            final Statement statement;

            try {
                statement = connection.createStatement();
                LOGGER.debug("Created statement #{}", statement.hashCode());
            } catch (SQLException throwables) {
                LOGGER.error("Cannot create statement", throwables);
                throw new RuntimeException(throwables);
            }

            LOGGER.debug("Call statement consumer #{}", consumer.hashCode());
            consumer.accept(statement);
        });
    }

}
