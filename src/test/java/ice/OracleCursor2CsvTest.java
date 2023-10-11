package ice;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleCursor2CsvTest extends Db2Csv {

    public static void main(String[] args) throws SQLException, IOException {

        Db2Csv.FindBindValueFunction findBindValueFunction = bindName -> {
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
