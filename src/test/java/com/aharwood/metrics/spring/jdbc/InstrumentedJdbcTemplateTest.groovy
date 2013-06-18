package com.aharwood.metrics.spring.jdbc

import com.yammer.metrics.core.MetricsRegistry
import org.apache.derby.jdbc.EmbeddedDriver
import org.junit.Before
import org.junit.Test
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.PreparedStatementCreator
import org.springframework.jdbc.core.PreparedStatementSetter
import org.springframework.jdbc.core.ResultSetExtractor
import org.springframework.jdbc.core.RowCallbackHandler
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.SqlProvider
import org.springframework.jdbc.datasource.SimpleDriverDataSource

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

import static junit.framework.TestCase.assertEquals

class InstrumentedJdbcTemplateTest {

    private InstrumentedJdbcTemplate jdbcTemplate

    @Before
    void setup() {
        this.jdbcTemplate = newJdbcTemplate()
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

    private InstrumentedJdbcTemplate newJdbcTemplate() {
        def dataSource = new SimpleDriverDataSource(new EmbeddedDriver(), "jdbc:derby:memory:TestDB;create=true")
        def jdbcTemplate = new InstrumentedJdbcTemplate(dataSource)
        def metricsRegistry = new MetricsRegistry()
        jdbcTemplate.setMetricsRegistry(metricsRegistry)
        jdbcTemplate
    }

    private void verify(String sql) {
        assertEquals(1, jdbcTemplate.metricsRegistry.allMetrics().size())
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
}
