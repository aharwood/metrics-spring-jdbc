metrics-spring-jdbc
===================

Binds Codahale Metrics into spring-jdbc methods

##Instrumenting [JdbcTemplate](http://static.springsource.org/spring/docs/3.1.x/javadoc-api/org/springframework/jdbc/core/JdbcTemplate.html)
`InstrumentedJdbcTemplate` is a subclass of `JdbcTemplate` and instruments all query, execute and update methods. The only addition
required is setting the `metricsRegistry` property in your Spring configuration.

Generally speaking, the class attempts to create a new metrics Timer per unique SQL query. This allows direct traceability between
a particular SQL statement and the time it took to run. It does not include parameter values in the instrumentation. Care should be
taken with dynamically generated queries (e.g. from an ORM library), since each one will generate its own metric.

It is also worth noting that with some queries (e.g. executing a stored procedure) it is not possible to fetch the actual SQL
being executed. In this case, the metric is stored under a generic name, indicating which method on JdbcTemplate invoked it.
