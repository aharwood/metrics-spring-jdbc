package com.aharwood.metrics.spring.jdbc

import com.yammer.metrics.core.MetricsRegistry
import org.apache.derby.jdbc.EmbeddedDriver
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.CallableStatementCallback
import org.springframework.jdbc.core.CallableStatementCreator
import org.springframework.jdbc.core.ConnectionCallback
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter
import org.springframework.jdbc.core.PreparedStatementCallback
import org.springframework.jdbc.core.PreparedStatementCreator
import org.springframework.jdbc.core.PreparedStatementSetter
import org.springframework.jdbc.core.ResultSetExtractor
import org.springframework.jdbc.core.RowCallbackHandler
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.SqlProvider
import org.springframework.jdbc.core.StatementCallback
import org.springframework.jdbc.datasource.SimpleDriverDataSource

import java.sql.CallableStatement
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

import static junit.framework.TestCase.assertEquals
import static junit.framework.TestCase.assertTrue

class InstrumentedJdbcTemplateTest {

    private static InstrumentedJdbcTemplate jdbcTemplate

    @BeforeClass
    static void setupJdbcTemplate() {
        def dataSource = new SimpleDriverDataSource(new EmbeddedDriver(), "jdbc:derby:memory:TestDB;create=true")
        jdbcTemplate = new InstrumentedJdbcTemplate(dataSource)
        jdbcTemplate.setMetricsRegistry(new MetricsRegistry())

        //create a test table
        jdbcTemplate.execute("CREATE TABLE Test (ID INT NOT NULL, NAME VARCHAR(32) NOT NULL)")
    }

    @Before
    void setup() {
        //reset the state of metrics
        jdbcTemplate.metricsRegistry.allMetrics().keySet().each {
            metricName -> jdbcTemplate.metricsRegistry.removeMetric(metricName)
        }
    }

    @Test
    void batchUpdate() {
        def sql = ["INSERT INTO Test VALUES (1, 'batchUpdate0')", "INSERT INTO Test VALUES (2, 'batchUpdate1')"] as String[]
        jdbcTemplate.batchUpdate(sql)
        verify()
    }

    @Test
    void batchUpdateBatchPreparedStatementSetter() {
        def sql = "INSERT INTO Test VALUES (1, 'batchUpdate0')"
        jdbcTemplate.batchUpdate(sql, new MockBatchPreparedStatementSetter())
        verify()
    }

    @Test
    void batchUpdateCollectionIntParameterizedPreparedStatementSetter() {
        def sql = "INSERT INTO Test VALUES (1, 'batchUpdate0')"
        jdbcTemplate.batchUpdate(sql, [], 1, new MockParameterizedPreparedStatementSetter())
        verify()
    }

    @Test
    void batchUpdateList() {
        def sql = "INSERT INTO Test VALUES (1, 'batchUpdate0')"
        jdbcTemplate.batchUpdate(sql, [])
        verify()
    }

    @Test
    void batchUpdateListIntArray() {
        def sql = "INSERT INTO Test VALUES (1, 'batchUpdate0')"
        jdbcTemplate.batchUpdate(sql, [], new int [0])
        verify()
    }

    @Test
    void call() {
        jdbcTemplate.call(new MockCallableStatementCreator(), [])
        verify()
    }

    @Test
    void executeCallableStatementCreatorCallableStatementCallback() {
        jdbcTemplate.execute(new MockCallableStatementCreator(), new MockCallableStatementCallback())
        verify()
    }

    @Test
    void executeConnectionCallback() {
        jdbcTemplate.execute(new MockConnectionCallback())
        verify()
    }

    @Test
    void executePreparedStatementCreatorPreparedStatementCallback() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.execute(new MockPreparedStatementCreator(sql), new MockPreparedStatementCallback())
        verify(sql)
    }

    @Test
    void executeStatementCallback() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.execute(new MockStatementCallback(sql))
        verify(sql)
    }

    @Test
    void executeString() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.execute(sql)
        verify(sql)
    }

    @Test
    void executeStringCallableStatementCallback() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.execute(sql, new MockCallableStatementCallback())
        verify(sql)
    }

    @Test
    void executeStringPreparedStatementCallback() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.execute(sql, new MockPreparedStatementCallback())
        verify(sql)
    }

    @Test
    void queryPreparedStatementCreatorPreparedStatementSetterResultSetExtractor() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(new MockPreparedStatementCreator(sql), new MockPreparedStatementSetter(), new MockResultSetExtractor())
        verify(sql)
    }

    @Test
    void queryPreparedStatementCreatorResultSetExtractor() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(new MockPreparedStatementCreator(sql), new MockResultSetExtractor())
        verify(sql)
    }

    @Test
    void queryPreparedStatementCreatorRowCallbackHandler() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(new MockPreparedStatementCreator(sql), new MockRowCallbackHandler())
        verify(sql)
    }

    @Test
    void queryPreparedStatementCreatorRowMapper() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(new MockPreparedStatementCreator(sql), new MockRowMapper())
        verify(sql)
    }

    @Test
    void queryStringObjectArrayIntArrayResultSetExtractor() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new Object[0], new int[0], new MockResultSetExtractor())
        verify(sql)
    }

    @Test
    void queryStringObjectArrayIntArrayRowCallbackHandler() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new Object[0], new int[0], new MockRowCallbackHandler())
        verify(sql)
    }

    @Test
    void queryStringObjectArrayIntArrayRowMapper() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new Object[0], new int[0], new MockRowMapper())
        verify(sql)
    }

    @Test
    void queryStringObjectArrayResultSetExtractor() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new Object[0], new MockResultSetExtractor())
        verify(sql)
    }

    @Test
    void queryStringObjectArrayRowCallbackHandler() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new Object[0], new MockRowCallbackHandler())
        verify(sql)
    }

    @Test
    void queryStringObjectArrayRowMapper() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new Object[0], new MockRowMapper())
        verify(sql)
    }

    @Test
    void queryStringPreparedStatementSetterResultSetExtractor() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new MockPreparedStatementSetter(), new MockResultSetExtractor())
        verify(sql)
    }

    @Test
    void queryStringPreparedStatementSetterRowCallbackHandler() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new MockPreparedStatementSetter(), new MockRowCallbackHandler())
        verify(sql)
    }

    @Test
    void queryStringPreparedStatementSetterRowMapper() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new MockPreparedStatementSetter(), new MockRowMapper())
        verify(sql)
    }

    @Test
    void queryStringResultSetExtractor() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new MockResultSetExtractor())
        verify(sql)
    }

    @Test
    void queryStringResultSetExtractorVarArgs() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new MockResultSetExtractor(), new Object[0])
        verify(sql)
    }

    @Test
    void queryStringRowCallbackHandler() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new MockRowCallbackHandler())
        verify(sql)
    }

    @Test
    void queryStringRowCallbackHandlerVarArgs() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new MockRowCallbackHandler(), new Object[0])
        verify(sql)
    }

    @Test
    void queryStringRowMapper() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new MockRowMapper())
        verify(sql)
    }

    @Test
    void queryStringRowMapperVarArgs() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.query(sql, new MockRowMapper(), new Object[0])
        verify(sql)
    }

    @Test
    void queryForIntString() {
        String sql = "select count(0) from SYS.SYSTABLES"
        jdbcTemplate.queryForInt(sql)
        verify(sql)
    }

    @Test
    void queryForIntStringVarArgs() {
        String sql = "select count(0) from SYS.SYSTABLES"
        jdbcTemplate.queryForInt(sql, new Object[0])
        verify(sql)
    }

    @Test
    void queryForIntStringObjectArrayIntArray() {
        String sql = "select count(0) from SYS.SYSTABLES"
        jdbcTemplate.queryForInt(sql, new Object[0], new int[0])
        verify(sql)
    }

    @Test
    void queryForList() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.queryForList(sql)
        verify(sql)
    }

    @Test
    void queryForListClass() {
        String sql = "select TABLETYPE from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForList(sql, String.class)
        verify(sql)
    }

    @Test
    void queryForListClassVarArgs() {
        String sql = "select TABLETYPE from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForList(sql, String.class, new Object[0])
        verify(sql)
    }

    @Test
    void queryForListVarArgs() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.queryForList(sql, new Object[0])
        verify(sql)
    }

    @Test
    void queryForListObjectArrayClass() {
        String sql = "select TABLETYPE from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForList(sql, new Object[0], String.class)
        verify(sql)
    }

    @Test
    void queryForListObjectArrayIntArray() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.queryForList(sql, new Object[0], new int[0])
        verify(sql)
    }

    @Test
    void queryForListObjectArrayIntArrayClass() {
        String sql = "select TABLETYPE from SYS.SYSTABLES"
        jdbcTemplate.queryForList(sql, new Object[0], new int[0], String.class)
        verify(sql)
    }

    @Test
    void queryForLong() {
        String sql = "select count(0) from SYS.SYSTABLES"
        jdbcTemplate.queryForLong(sql)
        verify(sql)
    }

    @Test
    void queryForLongVarArgs() {
        String sql = "select count(0) from SYS.SYSTABLES"
        jdbcTemplate.queryForLong(sql, new Object[0])
        verify(sql)
    }

    @Test
    void queryForLongObjectArrayIntArray() {
        String sql = "select count(0) from SYS.SYSTABLES"
        jdbcTemplate.queryForLong(sql, new Object[0], new int[0])
        verify(sql)
    }

    @Test
    void queryForMap() {
        String sql = "select * from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForMap(sql)
        verify(sql)
    }

    @Test
    void queryForMapVarArgs() {
        String sql = "select * from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForMap(sql, new Object[0])
        verify(sql)
    }

    @Test
    void queryForMapObjectArrayIntArray() {
        String sql = "select * from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForMap(sql, new Object[0], new int[0])
        verify(sql)
    }

    @Test
    void queryForObject() {
        String sql = "select TABLETYPE from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForObject(sql, String.class)
        verify(sql)
    }

    @Test
    void queryForObjectVarArgs() {
        String sql = "select TABLETYPE from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForObject(sql, String.class, new Object[0])
        verify(sql)
    }

    @Test
    void queryForObjectObjectArray() {
        String sql = "select TABLETYPE from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForObject(sql, new Object[0], String.class)
        verify(sql)
    }

    @Test
    void queryForObjectObjectArrayIntArray() {
        String sql = "select TABLETYPE from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForObject(sql, new Object[0], new int[0], String.class)
        verify(sql)
    }

    @Test
    void queryForObjectObjectArrayIntArrayRowMapper() {
        String sql = "select TABLETYPE from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForObject(sql, new Object[0], new int[0], new MockRowMapper())
        verify(sql)
    }

    @Test
    void queryForObjectObjectArrayRowMapper() {
        String sql = "select TABLETYPE from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForObject(sql, new Object[0], new MockRowMapper())
        verify(sql)
    }

    @Test
    void queryForObjectRowMapper() {
        String sql = "select TABLETYPE from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForObject(sql, new MockRowMapper())
        verify(sql)
    }

    @Test
    void queryForObjectRowMapperVarArgs() {
        String sql = "select TABLETYPE from SYS.SYSTABLES FETCH FIRST 1 ROWS ONLY"
        jdbcTemplate.queryForObject(sql, new MockRowMapper(), new Object[0])
        verify(sql)
    }

    @Test
    void queryForRowSet() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.queryForRowSet(sql)
        verify(sql)
    }

    @Test
    void queryForRowSetVarArgs() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.queryForRowSet(sql, new Object[0])
        verify(sql)
    }

    @Test
    void queryForRowSetObjectArrayIntArray() {
        String sql = "select * from SYS.SYSTABLES"
        jdbcTemplate.queryForRowSet(sql, new Object[0], new int[0])
        verify(sql)
    }

    private void verify() {
        assertEquals(1, jdbcTemplate.metricsRegistry.allMetrics().size())
    }

    private void verify(String... sql) {
        assertEquals(sql.size(), jdbcTemplate.metricsRegistry.allMetrics().size())
        sql.each { ->
            assertTrue(jdbcTemplate.metricsRegistry.allMetrics().keySet().contains(it))
        }
    }

    private void verify(String sql) {
        verify()
        assertEquals(sql, jdbcTemplate.metricsRegistry.allMetrics().keySet().iterator().next().name)
    }

    private static class MockRowMapper implements RowMapper {
        Object mapRow(ResultSet rs, int rowNum) throws SQLException {
            return null
        }
    }

    private static class MockRowCallbackHandler implements RowCallbackHandler {
        void processRow(ResultSet rs) throws SQLException {
        }
    }

    private static class MockResultSetExtractor implements ResultSetExtractor {
        Object extractData(ResultSet rs) throws SQLException, DataAccessException {
            return null
        }
    }

    private static class MockPreparedStatementSetter implements PreparedStatementSetter {
        void setValues(PreparedStatement ps) throws SQLException {
        }
    }

    private static class MockPreparedStatementCreator implements PreparedStatementCreator, SqlProvider {
        private String sql

        private MockPreparedStatementCreator(String sql) {
            this.sql = sql;
        }

        PreparedStatement createPreparedStatement(Connection con) throws SQLException {
            [
                executeQuery: {
                    [
                        next: { -> false }
                    ] as ResultSet
                }
            ] as PreparedStatement
        }

        String getSql() {
            this.sql
        }
    }

    private static class MockCallableStatementCreator implements CallableStatementCreator {
        CallableStatement createCallableStatement(Connection con) throws SQLException {
            return [
                execute: { -> false },
                getUpdateCount: { -> -1 },
                getMoreResults: { -> false }
            ] as CallableStatement
        }
    }

    private static class MockCallableStatementCallback implements CallableStatementCallback {
        Object doInCallableStatement(CallableStatement cs) throws SQLException, DataAccessException {
            null
        }
    }

    private static class MockConnectionCallback implements ConnectionCallback {
        Object doInConnection(Connection con) throws SQLException, DataAccessException {
            null
        }
    }

    private static class MockPreparedStatementCallback implements PreparedStatementCallback {
        Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
            null
        }
    }

    private static class MockStatementCallback implements StatementCallback, SqlProvider {
        private String sql

        private MockStatementCallback(String sql) {
            this.sql = sql;
        }

        Object doInStatement(Statement stmt) throws SQLException, DataAccessException {
            null
        }

        String getSql() {
            return sql
        }
    }

    private static class MockBatchPreparedStatementSetter implements BatchPreparedStatementSetter {
        int getBatchSize() {
            1
        }
        void setValues(PreparedStatement ps, int i) throws SQLException {
        }
    }

    private static class MockParameterizedPreparedStatementSetter implements ParameterizedPreparedStatementSetter {
        void setValues(PreparedStatement ps, Object argument) throws SQLException {
        }
    }
}
