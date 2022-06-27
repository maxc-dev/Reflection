import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class Deserializer<T> extends Serial<T> {
    private static final Logger log = LoggerFactory.getLogger(Deserializer.class);

    public Deserializer(Reflector<T> reflector, Config config) {
        super(reflector, config);
    }

    /**
     * Returns a list of objects of type T.
     */
    protected List<T> reflect(ResultSet rs) throws SQLException {
        List<T> serialized = new ArrayList<>();
        List<ReflectionMap> reflectionMaps = getFieldTypeMap(getColumnLabels(rs.getMetaData()));

        if (reflectionMaps.isEmpty()) {
            log.warn("No fields found in the result set. No deserialization will be performed.");
            return serialized;
        }

        log.info("Deserializing {} columns: {}", reflectionMaps.size(),
                reflectionMaps.stream().map(r -> r.columnName).collect(Collectors.joining(", ")));

        while (rs.next()) {
            T contents = createContents();
            for (ReflectionMap reflectionMap : reflectionMaps) {
                Object obj = reflectionMap.getValueFromRs(rs);
                try {
                    reflectionMap.getField().set(contents, obj);
                } catch (IllegalAccessException e) {
                    log.error("Failed to set field {} to value {}", reflectionMap.getField().getName(), obj);
                    throw new RuntimeException(e);
                }
            }
            serialized.add(contents);
        }
        log.info("Deserialized {} rows", serialized.size());
        return serialized;
    }

    /**
     * Creates a map between a Field and the column name it should get from the result set.
     */
    private List<ReflectionMap> getFieldTypeMap(Set<String> columnLabels) {
        return Arrays.stream(getClassType().getDeclaredFields())
                .filter(field -> columnLabelsContainsFieldName(columnLabels, field))
                .map(field -> new ReflectionMap(field, getColumnNameFromField(field)))
                .collect(Collectors.toList());
    }

    /**
     * Returns the column name for the given field.
     */
    private Set<String> getColumnLabels(ResultSetMetaData metaData) {
        int count;
        try {
            count = metaData.getColumnCount();
            Set<String> columnLabels = new HashSet<>(count);
            for (int i = 1; i <= count; i++) {
                columnLabels.add(metaData.getColumnLabel(i));
            }

            if (!columnLabels.isEmpty()) {
                return columnLabels;
            }
        } catch (SQLException e) {
            log.error("Error getting column labels", e);
            throw new RuntimeException(e);
        }
        return Collections.emptySet();
    }

    /**
     * Stores all the different fields, theiur column name, and the type of the field.
     */
    private static class ReflectionMap {
        private final Field field;
        private final String columnName;
        private final Class<?> type;

        public ReflectionMap(Field field, String columnName) {
            this.field = field;
            field.setAccessible(true);
            this.columnName = columnName;
            this.type = field.getType();
        }

        public Field getField() {
            return field;
        }

        private Object getValueFromRs(ResultSet rs) throws SQLException {
            if (int.class.equals(type) || Integer.class.equals(type)) {
                return rs.getInt(columnName);
            } else if (long.class.equals(type) || Long.class.equals(type)) {
                return rs.getLong(columnName);
            } else if (String.class.equals(type)) {
                return rs.getString(columnName);
            } else if (boolean.class.equals(type) || Boolean.class.equals(type)) {
                return rs.getBoolean(columnName);
            } else if (double.class.equals(type) || Double.class.equals(type)) {
                return rs.getDouble(columnName);
            } else if (float.class.equals(type) || Float.class.equals(type)) {
                return rs.getFloat(columnName);
            } else if (byte.class.equals(type) || Byte.class.equals(type)) {
                return rs.getByte(columnName);
            } else if (short.class.equals(type) || Short.class.equals(type)) {
                return rs.getShort(columnName);
            } else if (char.class.equals(type) || Character.class.equals(type)) {
                return rs.getString(columnName).charAt(0);
            } else if (LocalDate.class.equals(type)) {
                Date date = rs.getDate(columnName);
                return date != null ? date.toLocalDate() : null;
            } else {
                return rs.getObject(columnName);
            }
        }
    }
}
