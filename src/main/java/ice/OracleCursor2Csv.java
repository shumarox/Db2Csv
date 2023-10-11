package ice;

import oracle.jdbc.OracleTypes;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleCursor2Csv extends Db2Csv {

    public String cursorParameterPlaceHolderPattern = "\\$\\{cursor}";

    public String errorParameterPlaceHolderPattern = "\\$\\{error}";

    public String outParameterPlaceHolderPattern = "\\$\\{([^}]+?)}";

    public String getAllOutParameterPlaceHolderPattern() {
        return cursorParameterPlaceHolderPattern + "|" + errorParameterPlaceHolderPattern + "|" + outParameterPlaceHolderPattern;
    }

    private int cursorParameterIndex = -1;

    private int errorParameterIndex = -1;

    private Set<Integer> outParameterPlaceHolderIndexSet = new HashSet<>();

    @Override
    public PreparedStatement prepareStatement(Connection conn, String sql, Object[] binds) throws SQLException {

        Pattern pattern = Pattern.compile("\\?|" + getAllOutParameterPlaceHolderPattern());
        Matcher matcher = pattern.matcher(sql);

        int index = 1;

        while (matcher.find()) {
            String placeHolder = matcher.group(0);

            if (placeHolder.matches(cursorParameterPlaceHolderPattern)) {
                if (cursorParameterIndex != -1) {
                    throw new IllegalArgumentException(cursorParameterPlaceHolderPattern + "は1つのみ設定してください。");
                }
                cursorParameterIndex = index;
            } else if (placeHolder.matches(errorParameterPlaceHolderPattern)) {
                if (errorParameterIndex != -1) {
                    throw new IllegalArgumentException(errorParameterPlaceHolderPattern + "は1つのみ設定してください。");
                }
                errorParameterIndex = index;
            } else if (placeHolder.matches(outParameterPlaceHolderPattern)) {
                outParameterPlaceHolderIndexSet.add(index);
            }

            index++;
        }

        sql = sql.replaceAll(getAllOutParameterPlaceHolderPattern(), "?");

        if (cursorParameterIndex == -1) {
            throw new IllegalArgumentException("CURSOR型のOUT引数に" + cursorParameterPlaceHolderPattern + "を設定してください。");
        }

        CallableStatement statement = conn.prepareCall(sql);

        try {
            statement.setFetchSize(fetchSize);
        } catch (SQLException ex) {
            // 処理なし
        }

        statement.registerOutParameter(cursorParameterIndex, OracleTypes.CURSOR);

        if (errorParameterIndex >= 0) {
            statement.registerOutParameter(errorParameterIndex, OracleTypes.VARCHAR);
        }

        for (int outParamIndex : outParameterPlaceHolderIndexSet) {
            statement.registerOutParameter(outParamIndex, OracleTypes.VARCHAR);
        }

        int column = 1;

        for (Object value : binds) {
            while (column == cursorParameterIndex || column == errorParameterIndex || outParameterPlaceHolderIndexSet.contains(column)) {
                column++;
            }

            statement.setObject(column, value);
            column++;
        }

        return statement;
    }

    @Override
    protected ResultSet executeQuery(PreparedStatement statement) throws SQLException {
        CallableStatement cs = (CallableStatement) statement;
        cs.execute();

        if (errorParameterIndex >= 0) {
            String error = cs.getString(errorParameterIndex);
            if (error != null && !error.isEmpty()) {
                throw new RuntimeException(error);
            }
        }

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
                    throw new IllegalArgumentException("不明なバインド変数名です。" + bindName);
            }
        };

        OracleCursor2Csv db2Csv = new OracleCursor2Csv();
        db2Csv.charset = Charset.forName("Windows-31J");
        db2Csv.returnCode = "\r\n";

        String csvPathString = "C:\\temp\\test.csv";
        String jdbcUrlString = "jdbc:oracle:thin:test/test@192.168.0.99:1521/s12c";
        String sql = "{call test_cursor.get_cursor(${cursor}, #{cond.comments}, #{cond.id}, ${dummy}, ${dummy}, ${error})}";

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
                IN_ID IN NUMBER,
                OUT_DUMMY1 OUT NUMBER,
                OUT_DUMMY2 OUT DATE,
                ERROR OUT VARCHAR2
            );
        END TEST_CURSOR;
        /
        CREATE OR REPLACE PACKAGE BODY TEST_CURSOR
        IS
            PROCEDURE GET_CURSOR(
                CURSOR1 OUT CURSOR,
                IN_COMMENTS IN VARCHAR2,
                IN_ID IN NUMBER,
                OUT_DUMMY1 OUT NUMBER,
                OUT_DUMMY2 OUT DATE,
                ERROR OUT VARCHAR2
            )
            IS
            BEGIN
                IF IN_ID IS NULL THEN
                    ERROR := 'IDを指定してください';
                    RETURN;
                END IF;

                OPEN CURSOR1 FOR
                    SELECT IN_COMMENTS AS "COMMENTS", IN_ID AS "COND", A.* FROM TEST A
                    WHERE A.ID LIKE '%' || IN_ID || '%' ESCAPE '\'
                ;
            END;
        END TEST_CURSOR;
        /
    */
}
