package ice;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Db2Csv {

    @FunctionalInterface
    public interface OutputStreamSupplier {
        OutputStream get() throws IOException;
    }

    @FunctionalInterface
    public interface FindBindValueFunction {
        Object apply(String bindName);
    }

    public Charset charset = StandardCharsets.UTF_8;
    public String columnSeparator = ",";
    public String returnCode = "\n";
    public boolean outputColumnNames = true;
    public String placeHolderPattern = "#\\{([^}]+?)}";
    public String dateFormat = "yyyy/MM/dd HH:mm:ss";
    public String timestampFormat = "yyyy/MM/dd HH:mm:ss.SSS";
    public char[] hexCharacters = "0123456789ABCDEF".toCharArray();
    public int fetchSize = 1000;
    public int bufferSize = 1_000_000;

    protected static class ColumnInfo {

        public final int type;
        public final int scale;
        public final boolean isNumber;
        public final boolean isDate;

        public ColumnInfo(int type, int scale) {
            this.type = type;
            this.isNumber = type == Types.NUMERIC || type == Types.INTEGER || type == Types.DOUBLE || type == Types.DECIMAL;
            this.isDate = type == Types.DATE || type == Types.TIMESTAMP || type == Types.TIME || type == -101;
            this.scale = scale;
        }
    }

    public String toHexString(byte b) {
        return new String(new char[]{hexCharacters[b >> 4 & 0xF], hexCharacters[b & 0xF]});
    }

    public String toHexString(byte[] ab) {
        return IntStream.range(0, ab.length).mapToObj(i -> toHexString(ab[i])).collect(Collectors.joining());
    }

    protected String getRowCsv(ResultSet rs, List<ColumnInfo> columnInfoList) throws SQLException {

        StringBuilder sb = new StringBuilder();

        int columnCount = columnInfoList.size();

        for (int col = 1; col <= columnCount; col++) {
            ColumnInfo columnInfo = columnInfoList.get(col - 1);

            if (col >= 2) {
                sb.append(columnSeparator);
            }

            Object o = rs.getObject(col);

            String s;

            if (o == null) {
                s = "";
            } else if (o instanceof Clob) {
                Clob clob = (Clob) o;
                s = "\"" + clob.getSubString(1, (int) clob.length()).replaceAll("\"", "\"\"") + "\"";
            } else if (o instanceof Blob) {
                Blob blob = (Blob) o;
                s = "\"0x" + toHexString(blob.getBytes(1, (int) blob.length())) + "\"";
            } else if (columnInfo.isNumber) {
                if (columnInfo.scale > 0) {
                    if (o instanceof BigDecimal) {
                        s = ((BigDecimal) o).toPlainString();
                    } else if (o instanceof Double) {
                        s = BigDecimal.valueOf((Double) o).toPlainString();
                    } else if (o instanceof Float) {
                        s = BigDecimal.valueOf((Float) o).toPlainString();
                    } else {
                        s = rs.getObject(col).toString();
                    }
                } else {
                    s = String.valueOf(rs.getLong(col));
                }
            } else if (columnInfo.isDate) {
                Object tsValue = o;
                try {
                    tsValue = rs.getTimestamp(col);
                } catch (Exception ex) {
                    // 何もしない
                }

                if (columnInfo.scale == 0) {
                    s = new SimpleDateFormat(dateFormat).format(tsValue);
                } else {
                    s = new SimpleDateFormat(timestampFormat).format(tsValue);
                }

                s = "\"" + s + "\"";
            } else {
                s = "\"" + o.toString().replaceAll("\"", "\"\"") + "\"";
            }

            sb.append(s);
        }

        return sb.toString();
    }

    public PreparedStatement prepareStatement(Connection conn, String sql, Object[] binds) throws SQLException {
        PreparedStatement statement = conn.prepareStatement(sql);

        try {
            statement.setFetchSize(fetchSize);
        } catch (SQLException ex) {
            // 処理なし
        }

        for (int i = 0; i < binds.length; i++) {
            statement.setObject(i + 1, binds[i]);
        }

        return statement;
    }

    public PreparedStatement prepareStatement(Connection conn, String sql, Map<String, Object> bindMap) throws SQLException {
        return prepareStatement(conn, sql, bindMap::get);
    }

    public PreparedStatement prepareStatement(Connection conn, String sql, FindBindValueFunction findBindValueFunction) throws SQLException {

        Pattern pattern = Pattern.compile(placeHolderPattern);
        Matcher matcher = pattern.matcher(sql);

        HashMap<String, Object> bindMap = new HashMap<>();
        ArrayList<Object> bindValueList = new ArrayList<>();

        while (matcher.find()) {
            String bindName = matcher.group(1);
            Object bindValue;

            if (bindMap.containsKey(bindName)) {
                bindValue = bindMap.get(bindName);
            } else {
                bindValue = findBindValueFunction.apply(bindName);
                bindMap.put(bindName, bindValue);
            }

            bindValueList.add(bindValue);
        }

        sql = sql.replaceAll(placeHolderPattern, "?");

        return prepareStatement(conn, sql, bindValueList.toArray());
    }

    public int makeCsv(Connection conn, String sql, Object[] binds, OutputStreamSupplier outputStreamSupplier) throws SQLException, IOException {
        try (PreparedStatement statement = prepareStatement(conn, sql, binds)) {
            return makeCsv(statement, outputStreamSupplier);
        }
    }

    public int makeCsv(Connection conn, String sql, Map<String, Object> bindMap, OutputStreamSupplier outputStreamSupplier) throws SQLException, IOException {
        try (PreparedStatement statement = prepareStatement(conn, sql, bindMap)) {
            return makeCsv(statement, outputStreamSupplier);
        }
    }

    public int makeCsv(Connection conn, String sql, FindBindValueFunction findBindValueFunction, OutputStreamSupplier outputStreamSupplier) throws SQLException, IOException {
        try (PreparedStatement statement = prepareStatement(conn, sql, findBindValueFunction)) {
            return makeCsv(statement, outputStreamSupplier);
        }
    }

    protected ResultSet executeQuery(PreparedStatement statement) throws SQLException {
        return statement.executeQuery();
    }

    public int makeCsv(PreparedStatement statement, OutputStreamSupplier outputStreamSupplier) throws SQLException, IOException {

        try (ResultSet rs = executeQuery(statement)) {
            ResultSetMetaData rsMeta = rs.getMetaData();

            int colCount = rsMeta.getColumnCount();

            ArrayList<String> columnNameList = new ArrayList<>(colCount);

            for (int col = 1; col <= colCount; col++) {
                columnNameList.add(rsMeta.getColumnName(col));
            }

            String columnNameCsv = "\"" + String.join("\"" + columnSeparator + "\"", columnNameList) + "\"";

            ArrayList<ColumnInfo> columnInfoList = new ArrayList<>(colCount);

            for (int col = 1; col <= colCount; col++) {
                columnInfoList.add(new ColumnInfo(rsMeta.getColumnType(col), rsMeta.getScale(col)));
            }

            int rowCount = 0;

            try (PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(outputStreamSupplier.get(), charset), bufferSize))) {
                if (outputColumnNames) {
                    pw.print(columnNameCsv);
                    pw.print(returnCode);
                }

                while (rs.next()) {
                    rowCount++;
                    String csv = getRowCsv(rs, columnInfoList);
                    pw.print(csv);
                    pw.print(returnCode);
                }
            }

            return rowCount;
        }
    }

    // for debug
    public static void main(String[] args) throws SQLException, IOException {

        FindBindValueFunction findBindValueFunction = bindName -> {
            switch (bindName) {
                case "cond.comments":
                    return "テスト";
                case "cond.id":
                    return BigDecimal.valueOf(1);
                default:
                    return null;
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