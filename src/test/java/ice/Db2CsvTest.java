package ice;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Db2CsvTest {

    public static void main(String[] args) throws SQLException, IOException {

        Db2Csv.FindBindValueFunction findBindValueFunction = bindName -> {
            switch (bindName) {
                case "cond.comments":
                    return "テスト";
                case "cond.id":
                    return BigDecimal.valueOf(1);
                default:
                    throw new IllegalArgumentException("不明なバインド変数名です。" + bindName);
            }
        };

        Db2Csv db2Csv = new Db2Csv();
        db2Csv.charset = Charset.forName("Windows-31J");
        db2Csv.returnCode = "\r\n";

        String csvPathString = "C:\\temp\\test.csv";
        String jdbcUrlString = "jdbc:oracle:thin:test/test@192.168.0.99:1521/s12c";
        String sql = "select #{cond.comments} as \"comments\", #{cond.id} as \"cond\", a.* from TEST a where a.id like '%' || #{cond.id} || '%' escape '\\'";

        try (Connection conn = DriverManager.getConnection(jdbcUrlString)) {
            int count = db2Csv.makeCsv(conn, sql, findBindValueFunction, () -> Files.newOutputStream(Paths.get(csvPathString)));
            System.out.println("Count: " + count);
        }
    }
}