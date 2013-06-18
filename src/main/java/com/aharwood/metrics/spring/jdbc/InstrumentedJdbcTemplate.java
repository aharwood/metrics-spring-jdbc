package com.aharwood.metrics.spring.jdbc;

import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.SqlProvider;
import org.springframework.jdbc.core.StatementCallback;

public class InstrumentedJdbcTemplate extends JdbcTemplate {

    private static final String GROUP_NAME = "JdbcTemplate";

    private MetricsRegistry metricsRegistry;

    public InstrumentedJdbcTemplate() {
        super();
    }

    public InstrumentedJdbcTemplate(DataSource dataSource) {
        super(dataSource);
    }

    public InstrumentedJdbcTemplate(DataSource dataSource, boolean lazyInit) {
        super(dataSource, lazyInit);
    }

    public MetricsRegistry getMetricsRegistry() {
        return metricsRegistry;
    }

    public void setMetricsRegistry(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
    }

    public <T> T execute(StatementCallback<T> action) throws DataAccessException {
        Timer timer = null;
        if (action instanceof SqlProvider) {
            timer = startTimer((SqlProvider) action, "execute.StatementCallback");
        }

        T result = super.execute(action);

        if (timer != null) {
            timer.stop();
        }

        return result;
    }

	public <T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action) throws DataAccessException {
        Timer timer = null;
        if (psc instanceof SqlProvider) {
            timer = startTimer((SqlProvider) psc, "execute.PreparedStatementCreator.PreparedStatementCallback");
        }

        T result = super.execute(psc, action);

        if (timer != null) {
            timer.stop();
        }

        return result;
    }

    protected Timer startTimer(SqlProvider sqlProvider, String type) {
        Timer timer = this.metricsRegistry.newTimer(new MetricName(GROUP_NAME, type, sqlProvider.getSql()),
                                                    TimeUnit.MILLISECONDS,
                                                    TimeUnit.SECONDS);
        timer.time();
        return timer;
    }
}
