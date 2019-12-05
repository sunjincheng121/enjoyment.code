package org.apache.flink.python.connector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.stream.Collectors;

public class SocketTableSource implements StreamTableSource<Row> {

	private String hostname;
	private int port;
	private String lineDelimiter;
	private String fieldDelimiter;
	private String[] fieldNames;
	private TypeInformation[] fieldTypes;
	private boolean appendProctime;

	public final int MAX_RETRY = 3;

	private SocketTableSource(
			String hostname,
			int port,
			String lineDelimiter,
			String fieldDelimiter,
			String[] fieldNames,
			TypeInformation[] fieldTypes,
			boolean appendProctime) {
		this.hostname = hostname;
		this.port = port;
		this.lineDelimiter = lineDelimiter;
		this.fieldDelimiter = fieldDelimiter;
		assert fieldNames.length == fieldTypes.length;
		if (fieldDelimiter == null && fieldNames.length != 1) {
			throw new IllegalArgumentException("Field delimiter is not specified, only one field is supported!");
		}
		boolean notAllString = Arrays.stream(fieldTypes).filter(type -> !type.equals(Types.STRING))
			.collect(Collectors.toList()).size() > 0;
		if (notAllString) {
			throw new IllegalArgumentException("SocketTableSource only supports varchar type!");
		}
		this.appendProctime = appendProctime;
		if (appendProctime) {
			this.fieldNames = new String[fieldNames.length + 1];
			this.fieldTypes = new TypeInformation[fieldTypes.length + 1];
			System.arraycopy(fieldNames, 0, this.fieldNames, 0, fieldNames.length);
			System.arraycopy(fieldTypes, 0, this.fieldTypes, 0, fieldTypes.length);
			this.fieldNames[fieldNames.length] = "proctime";
			this.fieldTypes[fieldTypes.length] = Types.LONG;
		} else {
			this.fieldNames = fieldNames;
			this.fieldTypes = fieldTypes;
		}
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
		return env.socketTextStream(hostname, port, lineDelimiter, MAX_RETRY)
			.flatMap(new Spliter(fieldNames.length, fieldDelimiter, appendProctime))
			.returns(getReturnType());
	}

	@Override
	public TableSchema getTableSchema() {
		return new TableSchema(fieldNames, fieldTypes);
	}

	public static class Builder {
		private String hostname = "0.0.0.0";
		private int port = 9999;
		private String lineDelimiter = "\n";
		private String fieldDelimiter = null;
		private String[] fieldNames = {"f0"};
		private TypeInformation[] fieldTypes = {Types.STRING};
		private boolean appendProctime = false;

		public Builder(){}

		public Builder withHostname(String hostname) {
			this.hostname = hostname;
			return this;
		}

		public Builder withPort(int port) {
			this.port = port;
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

		public Builder withSchema(String[] fieldNames, TypeInformation[] fieldTypes) {
			this.fieldNames = fieldNames;
			this.fieldTypes = fieldTypes;
			return this;
		}

		public Builder appendProctime(boolean appendProctime) {
			this.appendProctime = appendProctime;
			return this;
		}

		public SocketTableSource build() {
			return new SocketTableSource(
				hostname, port, lineDelimiter, fieldDelimiter, fieldNames, fieldTypes, appendProctime);
		}
	}

	public static class Spliter implements FlatMapFunction<String, Row> {

		private int fieldCount;
		private String fieldDelimiter;
		private Row reuseRow;
		private boolean appendProctime;

		public Spliter(int fieldCount, String fieldDelimiter, boolean appendProctime) {
			this.fieldCount = fieldCount;
			this.fieldDelimiter = fieldDelimiter;
			this.reuseRow = new Row(fieldCount);
			this.appendProctime = appendProctime;
		}

		@Override
		public void flatMap(String s, Collector<Row> collector) throws Exception {
			if (appendProctime ) {
				reuseRow.setField(fieldCount -1, System.currentTimeMillis());
			}
			if (fieldDelimiter != null) {
				String[] strings = s.split(fieldDelimiter);
				for (int i = 0; i < fieldCount - 1; i++) {
					if (i < strings.length) {
						reuseRow.setField(i, strings[i]);
					} else {
						reuseRow.setField(i, null);
					}
				}
			} else {
				reuseRow.setField(0, s);
			}
			collector.collect(reuseRow);
		}
	}
}
