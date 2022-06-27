import java.lang.reflect.Field;
import java.util.Set;

public abstract class Serial<T> {
    protected final Reflector<T> reflector;
    protected final Config config;

    public Serial(Reflector<T> reflector, Config config) {
        this.reflector = reflector;
        this.config = config;
    }

    protected T createContents() {
        return reflector.getSupplier().get();
    }

    protected Class<?> getClassType() {
        return createContents().getClass();
    }

    protected String getColumnNameFromField(Field field) {
        return field.isAnnotationPresent(Attribute.class) ?
                field.getAnnotation(Attribute.class).name() : field.getName();
    }

    protected boolean columnLabelsContainsFieldName(Set<String> columnLabels, Field field) {
        final String columnLabel = getColumnNameFromField(field);
        return config.isCaseSensitive() ? columnLabels.contains(columnLabel) :
                columnLabels.stream().anyMatch(columnLabel::equalsIgnoreCase);
    }
}
