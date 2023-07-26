package ice;

import oracle.jdbc.OracleTypes;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;

public class OracleCursor2Csv extends Db2Csv {

    public int cursorParameterIndex = -1;

    @Override
    public PreparedStatement prepareStatement(Connection conn, String sql, Object[] binds) throws SQLException {
        if (cursorParameterIndex == -1) {
            throw new IllegalArgumentException("cursorParameterIndexを設定してください。");
        }

        CallableStatement statement = conn.prepareCall(sql);

        try {
            statement.setFetchSize(fetchSize);
        } catch (SQLException ex) {
            // 処理なし
        }

        int column = 1;

        for (Object value : binds) {
            if (column == cursorParameterIndex) {
                column++;
            }

            statement.setObject(column, value);
            column++;
        }

        statement.registerOutParameter(cursorParameterIndex, OracleTypes.CURSOR);

        return statement;
    }

    @Override
    protected ResultSet executeQuery(PreparedStatement statement) throws SQLException {
        CallableStatement cs = (CallableStatement) statement;
        cs.execute();
        return (ResultSet) cs.getObject(cursorParameterIndex);
    }

    // for debug
    public static void main(String[] args) throws SQLException, IOException {

        FindBindValueFunction findBindValueFunction = bindName -> {
            switch (bindName) {
                case "cond.comments":
                    return "テスト";
                case "cond.id":
                    return BigDecimal.valueOf(3);
                default:
                    return null;
            }
        };

        OracleCursor2Csv db2Csv = new OracleCursor2Csv();
        db2Csv.charset = Charset.forName("Windows-31J");
        db2Csv.returnCode = "\r\n";
        db2Csv.cursorParameterIndex = 1;

        String csvPathString = "C:\\temp\\test.csv";
        String jdbcUrlString = "jdbc:oracle:thin:test/test@192.168.0.99:1521/s12c";
        String sql = "{call test_cursor.get_cursor(?, #{cond.comments}, #{cond.id})}";

        try (Connection conn = DriverManager.getConnection(jdbcUrlString)) {
            int count = db2Csv.makeCsv(conn, sql, findBindValueFunction, () -> Files.newOutputStream(Paths.get(csvPathString)));
            System.out.println("Count: " + count);
        }
    }

    /*
        CREATE OR REPLACE PACKAGE TEST_CURSOR
        IS
            TYPE CURSOR IS REF CURSOR;
            PROCEDURE GET_CURSOR(
                CURSOR1 OUT CURSOR,
                IN_COMMENTS IN VARCHAR2,
                IN_ID IN NUMBER
            );
        END TEST_CURSOR;
        /
        CREATE OR REPLACE PACKAGE BODY TEST_CURSOR
        IS
            PROCEDURE GET_CURSOR(
                CURSOR1 OUT CURSOR,
                IN_COMMENTS IN VARCHAR2,
                IN_ID IN NUMBER
            )
            IS
            BEGIN
                OPEN CURSOR1 FOR
                    SELECT IN_COMMENTS AS "COMMENTS", IN_ID AS "COND", A.* FROM TEST A
                    WHERE A.ID LIKE '%' || IN_ID || '%' ESCAPE '\'
                ;
            END;
        END TEST_CURSOR;
        /
    */
}
