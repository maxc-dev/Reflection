import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Serializer<T> extends Serial<T> {
    private static final Logger log = LoggerFactory.getLogger(Serializer.class);

    private static final String BASE_QUERY = "INSERT INTO %s (%s) VALUES %s";
    private static final String NULL_VALUE = "NULL";
    private static final String CONVERT_DATE_FORMAT = "CONVERT(DATE,'%s')";
    private static final String CONVERT_DATETIME_FORMAT = "CONVERT(DATETIME,'%s')";

    public Serializer(Reflector<T> reflector, Config config) {
        super(reflector, config);
    }

    /**
     * Returns an SQL string from a collection oj objects of type T.
     */
    protected String serializeToSql(Collection<T> objects, String tableName, Set<String> fieldFilter) {
        if (objects.isEmpty()) {
            log.warn("No objects to serialize");
            return "";
        }

        LinkedHashMap<String, Field> classFields = getClassFields();
        if (fieldFilter != null) {
            classFields = classFields.entrySet().stream()
                    .filter(e -> columnLabelsContainsFieldName(fieldFilter, e.getValue()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (u, v) -> u, LinkedHashMap::new));
        }

        if (classFields.isEmpty()) {
            log.warn("No fields found in the class. No serialization will be performed.");
            return "";
        }

        log.info("Serializing {} fields: {}", classFields.size(),
                String.join(", ", classFields.keySet()));

        return String.format(BASE_QUERY, tableName,
                String.join(", ", classFields.keySet()),
                String.join(",", getFormattedValues(objects, classFields)));
    }

    /**
     * Returns a list of SQL strings from a collection of objects of type T.
     */
    private List<String> getFormattedValues(Collection<T> objects, LinkedHashMap<String, Field> classFields) {
        List<String> formattedValues = new ArrayList<>();

        objects.forEach(obj -> {
            List<String> values = new ArrayList<>();
            classFields.forEach((key, field) -> {
                try {
                    values.add(getValueFromObject(field, obj));
                } catch (IllegalAccessException e) {
                    log.error("Failed to get value from field {}", field.getName());
                    throw new RuntimeException(e);
                }
            });
            formattedValues.add(String.format("(%s)", String.join(",", values)));
        });
        return formattedValues;
    }

    /**
     * Extracts the value of a field for a given object.
     */
    private String getValueFromObject(Field field, T object) throws IllegalAccessException {
        if (field == null || field.get(object) == null) {
            return NULL_VALUE;
        }
        Class<?> type = field.getType();
        Object obj = field.get(object);
        if (type == Timestamp.class) {
            Timestamp timestamp = (Timestamp) obj;
            return formatDate(timestamp.toLocalDateTime(), true);
        } else if (type == LocalDate.class) {
            LocalDate localDate = (LocalDate) obj;
            return formatDate(localDate, false);
        } else if (isNonStringType(type)) {
            return String.valueOf(obj);
        } else {
            return String.format("'%s'", obj);
        }
    }

    /**
     * Returns true if the type is a non-string type.
     */
    private boolean isNonStringType(Class<?> type) {
        return type == int.class || type == long.class || type == double.class
                || type == float.class || (type == boolean.class && !config.isWrapBooleansInQuotes());
    }

    /**
     * Gets a set of the field names in a class and maps them to their field.
     */
    private LinkedHashMap<String, Field> getClassFields() {
        return Arrays.stream(getClassType().getDeclaredFields())
                .peek(f -> f.setAccessible(true))
                .filter(f -> !Modifier.isStatic(f.getModifiers()))
                .collect(Collectors.toMap(this::getColumnNameFromField, Function.identity(), (u, v) -> u, LinkedHashMap::new));
    }

    /**
     * Formats a date object to a convert SQL statement.
     */
    private String formatDate(TemporalAccessor date, boolean includeTime) {
        return String.format(includeTime ? CONVERT_DATETIME_FORMAT : CONVERT_DATE_FORMAT,
                includeTime ? config.getDateTimeFormat().format(date) : config.getDateFormat().format(date));
    }
}
