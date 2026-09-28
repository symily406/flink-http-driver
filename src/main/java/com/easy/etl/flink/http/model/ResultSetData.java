package com.easy.etl.flink.http.model;

import com.easy.etl.flink.http.util.Json;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 结果页的 {@code results} 对象：该页的列加行。
 */
public final class ResultSetData {

    private static final ResultSetData EMPTY =
            new ResultSetData(Collections.<Column>emptyList(), Collections.<Row>emptyList());

    private final List<Column> columns;
    private final List<Row> rows;

    public ResultSetData(List<Column> columns, List<Row> rows) {
        this.columns = Collections.unmodifiableList(new ArrayList<Column>(
                columns == null ? Collections.<Column>emptyList() : columns));
        this.rows = Collections.unmodifiableList(new ArrayList<Row>(
                rows == null ? Collections.<Row>emptyList() : rows));
    }

    @SuppressWarnings("unchecked")
    public static ResultSetData fromJson(Object json) {
        if (!(json instanceof Map)) {
            return EMPTY;
        }
        Map<String, Object> results = (Map<String, Object>) json;
        List<Column> cols = new ArrayList<Column>();
        Object colsObj = Json.get(results, "columns");
        if (colsObj instanceof List) {
            for (Object c : (List<?>) colsObj) {
                cols.add(Column.fromJson(c));
            }
        }
        List<Row> rows = new ArrayList<Row>();
        Object dataObj = Json.get(results, "data");
        if (dataObj instanceof List) {
            for (Object r : (List<?>) dataObj) {
                rows.add(Row.fromJson(r));
            }
        }
        return new ResultSetData(cols, rows);
    }

    public static ResultSetData empty() {
        return EMPTY;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<Row> getRows() {
        return rows;
    }

    /** 按顺序返回列标签；{@code null} 标签保持 {@code null}。 */
    public List<String> getColumnNames() {
        List<String> names = new ArrayList<String>(columns.size());
        for (Column c : columns) {
            names.add(c.getName());
        }
        return names;
    }

    public int getRowCount() {
        return rows.size();
    }

    public boolean isEmpty() {
        return rows.isEmpty();
    }

    @Override
    public String toString() {
        return "ResultSetData{columns=" + getColumnNames() + ", rows=" + rows.size() + "}";
    }
}
