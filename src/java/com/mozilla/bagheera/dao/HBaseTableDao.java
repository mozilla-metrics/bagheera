package com.mozilla.bagheera.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.mozilla.bagheera.util.IdUtil;

public class HBaseTableDao {

	private HTablePool pool;
	private byte[] tableName;
	private byte[] family;
	private byte[] qualifier;

	public HBaseTableDao(HTablePool pool, String tableName, String family, String qualifier) {
		this.pool = pool;
		this.tableName = Bytes.toBytes(tableName);
		this.family = Bytes.toBytes(family);
		this.qualifier = Bytes.toBytes(qualifier);
	}

	public void put(String value) throws IOException {
		HTableInterface table = null;
		try {
			table = pool.getTable(tableName);
			byte[] id = IdUtil.generateBucketizedId();
			Put p = new Put(id);
			p.add(family, qualifier, Bytes.toBytes(value));
			table.put(p);
		} finally {
			if (table != null) {
				pool.putTable(table);
			}
		}
	}

	public void put(String key, String value) throws IOException {
		HTableInterface table = null;
		try {
			table = pool.getTable(tableName);
			Put p = new Put(Bytes.toBytes(key));
			p.add(family, qualifier, Bytes.toBytes(value));
			table.put(p);
		} finally {
			if (table != null) {
				pool.putTable(table);
			}
		}
	}

	public void batchPut(List<String> values) throws IOException {
		HTable table = null;
		try {
			table = (HTable)pool.getTable(tableName);
			table.setAutoFlush(false);
			List<Put> puts = new ArrayList<Put>();
			for (String value : values) {
				byte[] id = IdUtil.generateBucketizedId();
				Put p = new Put(id);
				p.add(family, qualifier, Bytes.toBytes(value));
				puts.add(p);
			}
			table.put(puts);
			table.flushCommits();
		} finally {
			if (table != null) {
				pool.putTable(table);
			}
		}
	}

	public void batchPut(Map<String, String> values) throws IOException {
		HTable table = null;
		try {
			table = (HTable)pool.getTable(tableName);
			table.setAutoFlush(false);
			List<Put> puts = new ArrayList<Put>();
			for (Map.Entry<String, String> entry : values.entrySet()) {
				Put p = new Put(Bytes.toBytes(entry.getKey()));
				p.add(family, qualifier, Bytes.toBytes(entry.getValue()));
				puts.add(p);
			}
			table.put(puts);
			table.flushCommits();
		} finally {
			if (table != null) {
				pool.putTable(table);
			}
		}
	}

}
