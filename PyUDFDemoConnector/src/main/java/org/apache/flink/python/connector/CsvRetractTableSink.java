package org.apache.flink.python.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CsvRetractTableSink implements RetractStreamTableSink<Row> {

	private String filePath;
	private String lineDelimiter;
	private String fieldDelimiter;
	private String[] fieldNames;
	private TypeInformation<?>[] fieldTypes;

	private CsvRetractTableSink(String filePath, String lineDelimiter, String fieldDelimiter) {
		this.filePath = filePath;
		this.lineDelimiter = lineDelimiter;
		this.fieldDelimiter = fieldDelimiter;
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		return dataStream.addSink(new RowSink(filePath, lineDelimiter, fieldDelimiter)).setParallelism(1);
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		CsvRetractTableSink copy = new CsvRetractTableSink(filePath, lineDelimiter, fieldDelimiter);
		copy.fieldNames = fieldNames;
		copy.fieldTypes = fieldTypes;
		return copy;
	}

	private static class RowSink implements SinkFunction<Tuple2<Boolean, Row>> {

		private String filePath;
		private String lineDelimiter;
		private String fieldDelimiter;
		private List<String> lines = new ArrayList<>();

		public RowSink(String filePath, String lineDelimiter, String fieldDelimiter) {
			this.filePath = filePath;
			this.lineDelimiter = lineDelimiter;
			this.fieldDelimiter = fieldDelimiter;
		}

		@Override
		public void invoke(Tuple2<Boolean, Row> value) throws Exception {
			String[] fields = new String[value.f1.getArity()];
			for (int i = 0; i < value.f1.getArity(); i++) {
				fields[i] = String.valueOf(value.f1.getField(i));
			}
			String line = String.join(fieldDelimiter, fields);
			if (value.f0) {
				lines.add(line);
				Collections.sort(lines);
				try (OutputStream out = new FileOutputStream(filePath)) {
					for (String textLine : lines) {
						out.write(textLine.getBytes(Charset.forName("utf8")));
						out.write(lineDelimiter.getBytes(Charset.forName("utf8")));
					}
				}
			} else {
				lines.remove(line);
			}
		}
	}

	public static class Builder {
		private String filePath;
		private String lineDelimiter = "\n";
		private String fieldDelimiter = ",";
		private String[] fieldNames;
		private TypeInformation<?>[] fieldTypes;

		public Builder withFilePath(String filePath) {
			this.filePath = filePath;
			return this;
		}

		public Builder withLineDelimiter(String lineDelimiter) {
			this.lineDelimiter = lineDelimiter;
			return this;
		}

		public Builder withFieldDelimiter(String fieldDelimiter) {
			this.fieldDelimiter = fieldDelimiter;
			return this;
		}

		public Builder withSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
			this.fieldNames = fieldNames;
			this.fieldTypes = fieldTypes;
			return this;
		}

		public CsvRetractTableSink build() {
			TableSink<Tuple2<Boolean, Row>> sink = new CsvRetractTableSink(filePath, lineDelimiter, fieldDelimiter);
			if (fieldNames != null && fieldTypes != null) {
				sink = sink.configure(fieldNames, fieldTypes);
			}
			return (CsvRetractTableSink) sink;
		}
	}
}
