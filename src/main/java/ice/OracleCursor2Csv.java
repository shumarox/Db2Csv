package ice;

import oracle.jdbc.OracleTypes;

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
}
