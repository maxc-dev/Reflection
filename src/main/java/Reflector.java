import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class Reflector<T> {
    private final Supplier<T> supplier;
    private final Config config = new Config();

    public Reflector(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    protected Supplier<T> getSupplier() {
        return supplier;
    }

    public Config getConfig() {
        return config;
    }

    /**
     * Returns a list of objects of type T.
     *
     * @param rs The result set to deserialize.
     * @return A list of objects of type T.
     * @throws SQLException If an error occurs while deserializing.
     */
    public List<T> deserialize(ResultSet rs) throws SQLException {
        Deserializer<T> deserializer = new Deserializer<>(this, config);
        return deserializer.reflect(rs);
    }

    /**
     * Returns an SQL string from a collection oj objects of type T with a set of fields to only include.
     */
    public String serialize(Collection<T> objects, String tableName, Set<String> fieldFilter) {
        Serializer<T> serializer = new Serializer<>(this, config);
        return serializer.serializeToSql(objects, tableName, fieldFilter);
    }

    /**
     * Returns an SQL string from a collection oj objects of type T.
     */
    public String serialize(Collection<T> objects, String tableName) {
        return serialize(objects, tableName, null);
    }
}
