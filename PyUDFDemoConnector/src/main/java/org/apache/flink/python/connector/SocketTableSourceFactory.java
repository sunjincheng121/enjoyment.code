package org.apache.flink.python.connector;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

public class SocketTableSourceFactory implements StreamTableSourceFactory<Row>{

	public static final String SOCKET = "socket";
	public static final String PORT = "socket.port";
	public static final String HOSTNAME = "socket.hostname";
	public static final String APPEND_PROCTIME = "socket.append-proctime";
	public static final String FORMAT_FIELD_DELIMITER = "format.field-delimiter";
	public static final String FORMAT_LINE_DELIMITER = "format.line-delimiter";

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> map) {
		DescriptorProperties params = new DescriptorProperties(true);
		params.putProperties(map);
		new SchemaValidator(true, true, true).validate(params);
		TableSchema tableSchema = params.getTableSchema(SCHEMA);
		SocketTableSource.Builder builder = new SocketTableSource.Builder();
		builder.withHostname(params.getString(HOSTNAME));
		builder.withPort(params.getInt(PORT));
		builder.withSchema(tableSchema.getFieldNames(), tableSchema.getFieldTypes());
		if (params.containsKey(FORMAT_LINE_DELIMITER)) {
			builder.withLineDelimiter(params.getString(FORMAT_LINE_DELIMITER));
		}
		if (params.containsKey(FORMAT_FIELD_DELIMITER)) {
			builder.withFieldDelimiter(params.getString(FORMAT_FIELD_DELIMITER));
		}
		if (params.containsKey(APPEND_PROCTIME)) {
			builder.appendProctime(params.getBoolean(APPEND_PROCTIME));
		}
		return builder.build();
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, SOCKET);
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// host, port, lineDelimiter, fieldDelimiter, appendProctime
		properties.add(HOSTNAME);
		properties.add(PORT);
		properties.add(FORMAT_LINE_DELIMITER);
		properties.add(FORMAT_FIELD_DELIMITER);
		properties.add(APPEND_PROCTIME);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);
		properties.add(SCHEMA + ".#." + SCHEMA_FROM);

		// time attributes
		properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);
		return properties;
	}
}
