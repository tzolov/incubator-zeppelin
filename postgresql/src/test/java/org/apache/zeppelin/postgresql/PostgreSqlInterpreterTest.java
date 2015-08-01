/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.zeppelin.postgresql;

import static org.apache.zeppelin.postgresql.PostgreSqlInterpreter.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.zeppelin.interpreter.InterpreterResult;
import org.junit.Before;
import org.junit.Test;

import com.mockrunner.jdbc.BasicJDBCTestCaseAdapter;
import com.mockrunner.jdbc.StatementResultSetHandler;
import com.mockrunner.mock.jdbc.MockConnection;
import com.mockrunner.mock.jdbc.MockResultSet;

/**
 * PostgreSQL interpreter unit tests
 */
public class PostgreSqlInterpreterTest extends BasicJDBCTestCaseAdapter {

  private PostgreSqlInterpreter psqlInterpreter = null;
  private MockResultSet result = null;

  @Before
  public void beforeTest() {
    MockConnection connection = getJDBCMockObjectFactory().getMockConnection();

    StatementResultSetHandler statementHandler = connection.getStatementResultSetHandler();
    result = statementHandler.createResultSet();
    statementHandler.prepareGlobalResultSet(result);

    psqlInterpreter = spy(new PostgreSqlInterpreter(new Properties()));
    when(psqlInterpreter.getJdbcConnection()).thenReturn(connection);
  }

  
  @Test
  public void testDefaultProperties() throws SQLException {

    PostgreSqlInterpreter psqlInterpreter = new PostgreSqlInterpreter(new Properties());    

    assertEquals(DEFAULT_JDBC_DRIVER_NAME, psqlInterpreter.getProperty(POSTGRESQL_SERVER_DRIVER_NAME));
    assertEquals(DEFAULT_JDBC_URL, psqlInterpreter.getProperty(POSTGRESQL_SERVER_URL));
    assertEquals(DEFAULT_JDBC_USER_NAME, psqlInterpreter.getProperty(POSTGRESQL_SERVER_USER));
    assertEquals(DEFAULT_JDBC_USER_PASSWORD, psqlInterpreter.getProperty(POSTGRESQL_SERVER_PASSWORD));
  }

  @Test
  public void testConnectionClose() throws SQLException {

    PostgreSqlInterpreter psqlInterpreter = spy(new PostgreSqlInterpreter(new Properties()));

    when(psqlInterpreter.getJdbcConnection()).thenReturn(
        getJDBCMockObjectFactory().getMockConnection());

    psqlInterpreter.close();

    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
    verifyConnectionClosed();
  }

  @Test
  public void testStatementCancel() throws SQLException {

    PostgreSqlInterpreter psqlInterpreter = spy(new PostgreSqlInterpreter(new Properties()));

    when(psqlInterpreter.getJdbcConnection()).thenReturn(
        getJDBCMockObjectFactory().getMockConnection());

    psqlInterpreter.cancel(null);

    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
    assertFalse("Cancel operation should not close the connection", psqlInterpreter
        .getJdbcConnection().isClosed());
  }

  @Test
  public void testSelectQuery() throws SQLException {

    String sqlQuery = "select * from t";

    result.addColumn("col1", new String[] {"val11", "val12"});
    result.addColumn("col2", new String[] {"val21", "val22"});

    InterpreterResult interpreterResult = psqlInterpreter.interpret(sqlQuery, null);

    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TABLE, interpreterResult.type());
    assertEquals("col1\tcol2\nval11\tval21\nval12\tval22\n", interpreterResult.message());

    verifySQLStatementExecuted(sqlQuery);
    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
  }

  @Test
  public void testSelectQueryWithSpecialCharacters() throws SQLException {

    String sqlQuery = "select * from t";

    result.addColumn("co\tl1", new String[] {"val11", "va\tl1\n2"});
    result.addColumn("co\nl2", new String[] {"v\nal21", "val\t22"});

    InterpreterResult interpreterResult = psqlInterpreter.interpret(sqlQuery, null);

    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TABLE, interpreterResult.type());
    assertEquals("co l1\tco l2\nval11\tv al21\nva l1 2\tval 22\n", interpreterResult.message());

    verifySQLStatementExecuted(sqlQuery);
    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
  }

  @Test
  public void testExplainQuery() throws SQLException {

    String sqlQuery = "explain select * from t";

    result.addColumn("col1", new String[] {"val11", "val12"});

    InterpreterResult interpreterResult = psqlInterpreter.interpret(sqlQuery, null);

    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TEXT, interpreterResult.type());
    assertEquals("col1\nval11\nval12\n", interpreterResult.message());

    verifySQLStatementExecuted(sqlQuery);
    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
  }

  @Test
  public void testExplainQueryWithSpecialCharachters() throws SQLException {

    String sqlQuery = "explain select * from t";

    result.addColumn("co\tl\n1", new String[] {"va\nl11", "va\tl\n12"});

    InterpreterResult interpreterResult = psqlInterpreter.interpret(sqlQuery, null);

    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TEXT, interpreterResult.type());
    assertEquals("co\tl\n1\nva\nl11\nva\tl\n12\n", interpreterResult.message());

    verifySQLStatementExecuted(sqlQuery);
    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
  }
}
