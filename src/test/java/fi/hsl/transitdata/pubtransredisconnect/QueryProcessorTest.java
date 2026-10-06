package fi.hsl.transitdata.pubtransredisconnect;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

public class QueryProcessorTest {

    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    private AbstractResultSetProcessor<String> processor;
    private QueryProcessor queryProcessor;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        connection = mock(Connection.class);
        statement = mock(Statement.class);
        resultSet = mock(ResultSet.class);
        processor = mock(AbstractResultSetProcessor.class);

        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.getStatement()).thenReturn(statement);
        // Behaves like a real ResultSet: closed after the first close()
        when(resultSet.isClosed()).thenReturn(false, true);
        when(processor.getQuery()).thenReturn("SELECT 1");
        when(processor.collectResults(resultSet)).thenReturn(List.of("a", "b"));

        queryProcessor = new QueryProcessor(connection);
    }

    @Test
    public void closesResultSetAndStatementBeforeProcessingItems() throws Exception {
        queryProcessor.executeAndProcessQuery(processor);

        InOrder order = inOrder(statement, processor, resultSet);
        order.verify(statement).executeQuery("SELECT 1");
        order.verify(processor).collectResults(resultSet);
        order.verify(resultSet).close();
        order.verify(statement).close();
        order.verify(processor).processItems(List.of("a", "b"));
    }

    @Test
    public void closesOnlyOnceWhenFinallyBlockSeesClosedResultSet() throws Exception {
        queryProcessor.executeAndProcessQuery(processor);

        verify(resultSet, times(1)).close();
        verify(statement, times(1)).close();
    }

    @Test
    public void queryFailureIsSwallowedAndNothingIsProcessed() throws Exception {
        when(statement.executeQuery(anyString())).thenThrow(new SQLException("boom"));

        queryProcessor.executeAndProcessQuery(processor);

        verify(processor, never()).collectResults(any());
        verify(processor, never()).processItems(any());
    }

    @Test
    public void collectFailureIsSwallowedAndResultSetClosed() throws Exception {
        when(resultSet.isClosed()).thenReturn(false);
        when(processor.collectResults(resultSet)).thenThrow(new SQLException("boom"));

        queryProcessor.executeAndProcessQuery(processor);

        verify(processor, never()).processItems(any());
        verify(resultSet).close();
        verify(statement).close();
    }

    @Test
    public void processingFailureIsSwallowed() throws Exception {
        doThrow(new RuntimeException("redis down")).when(processor).processItems(any());

        queryProcessor.executeAndProcessQuery(processor);

        verify(processor).processItems(List.of("a", "b"));
    }

    @Test
    public void closeQueryIgnoresNull() {
        QueryProcessor.closeQuery(null);
    }

    @Test
    public void closeQuerySkipsAlreadyClosedResultSet() throws Exception {
        ResultSet closed = mock(ResultSet.class);
        when(closed.isClosed()).thenReturn(true);

        QueryProcessor.closeQuery(closed);

        verify(closed, never()).close();
        verify(closed, never()).getStatement();
    }

    @Test
    public void closeQueryClosesStatementEvenIfResultSetCloseFails() throws Exception {
        ResultSet failing = mock(ResultSet.class);
        when(failing.getStatement()).thenReturn(statement);
        doThrow(new SQLException("close failed")).when(failing).close();

        QueryProcessor.closeQuery(failing);

        verify(statement).close();
    }

    @Test
    public void closeQueryToleratesMissingStatement() throws Exception {
        ResultSet noStatement = mock(ResultSet.class);
        when(noStatement.getStatement()).thenThrow(new SQLException("no statement"));

        QueryProcessor.closeQuery(noStatement);

        verify(noStatement).close();
        verifyNoInteractions(statement);
    }

    @Test
    public void closeQueryClosesWhenIsClosedCheckFails() throws Exception {
        ResultSet unknown = mock(ResultSet.class);
        when(unknown.isClosed()).thenThrow(new SQLException("unknown"));
        when(unknown.getStatement()).thenReturn(statement);

        QueryProcessor.closeQuery(unknown);

        verify(unknown).close();
        verify(statement).close();
    }

    @Test
    public void statementCloseFailureIsSwallowed() throws Exception {
        doThrow(new SQLException("close failed")).when(statement).close();

        queryProcessor.executeAndProcessQuery(processor);

        verify(processor).processItems(List.of("a", "b"));
    }
}
