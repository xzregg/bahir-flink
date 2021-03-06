/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.connectors.kudu.table;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connectors.kudu.connector.KuduFilterInfo;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class KuduLookupFunction extends AsyncTableFunction<Row> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;
    private final KuduReaderConfig readerConfig;
    private final KuduTableInfo tableInfo;
    private final TypeInformation[] keyTypes;
    private List<KuduFilterInfo> tableFilters;
    private List<String> tableProjections;
    private final String[] keyNames;

    private final long cacheMaxSize;
    private final long cacheExpireMs;

    private transient Cache<Row, List<Row>> cache;
    private transient AsyncKuduClient client;
    private transient AsyncKuduSession session;
    private transient KuduTable table;


    public KuduLookupFunction(KuduReaderConfig readerConfig, KuduTableInfo tableInfo, TableSchema flinkSchema, List<KuduFilterInfo> tableFilters, List<String> tableProjections, String[] keyNames) {

        this.readerConfig = checkNotNull(readerConfig, "readerConfig could not be null");
        this.tableInfo = checkNotNull(tableInfo, "tableInfo could not be null");
        this.tableFilters = checkNotNull(tableFilters, "tableFilters could not be null");
        this.keyNames = keyNames;
        this.tableProjections = tableProjections;
        this.fieldNames = flinkSchema.getFieldNames();
        this.fieldTypes = flinkSchema.getFieldTypes();
        this.cacheMaxSize = readerConfig.getCacheMaxSize();
        this.cacheExpireMs = readerConfig.getCacheExpireMs();
        List<String> nameList = Arrays.asList(fieldNames);
        this.keyTypes = Arrays.stream(keyNames).map(s -> {
            checkArgument(nameList.contains(s),
                    "keyName %s can't find in fieldNames %s.", s, nameList);
            return fieldTypes[nameList.indexOf(s)];
        })
                .toArray(TypeInformation[]::new);
    }


    private Row toFlinkRow(RowResult row) {
        Schema schema = row.getColumnProjection();

        Row values = new Row(schema.getColumnCount());
        schema.getColumns().forEach(column -> {
            String name = column.getName();
            int pos = schema.getColumnIndex(name);
            //log.debug("value:" + name + "=" + row.getObject(name).toString());
            values.setField(pos, row.getObject(name));
        });
        return values;
    }


    public void eval(CompletableFuture<Collection<Row>> future, Object... keys) throws KuduException {
        Row keyRow = Row.of(keys);
        try {

            if (cache != null) {
                List<Row> cachedRows = cache.getIfPresent(keyRow);
                if (cachedRows != null) {
                    future.complete(cachedRows);
                    return;
                }
            }
            AsyncKuduScanner.AsyncKuduScannerBuilder asyncKuduScannerBuilder = client.newScannerBuilder(table);
            List<String> projectColumns = new ArrayList<>();
            //Schema schema = tableInfo.getSchema();
            Schema schema = table.getSchema();
            asyncKuduScannerBuilder.setProjectedColumnNames(tableProjections);

            log.debug("add filter data ------keys.length" + keys.length + " tableProjections:" + tableProjections.size());
            for (int i = 0; i < keys.length; i++) {
                ColumnSchema columnSchema = schema.getColumn(keyNames[i]);
                String columnName = columnSchema.getName();
                Object value = keys[i];

                KuduPredicate predicate = KuduPredicate.newComparisonPredicate(
                        columnSchema,
                        KuduPredicate.ComparisonOp.EQUAL,
                        value);
                asyncKuduScannerBuilder.addPredicate(predicate);
                projectColumns.add(columnName);

            }

            asyncKuduScannerBuilder.limit(100);
            AsyncKuduScanner asyncKuduScanner = asyncKuduScannerBuilder.build();
            List<Row> rowList = Lists.newArrayList();
            Deferred<RowResultIterator> iteratorDeferred = asyncKuduScanner.nextRows();
            iteratorDeferred.addCallback(
                    new GetListRow(
                            cache
                            , rowList
                            , asyncKuduScanner
                            , future
                            , keyRow
                    )
            );

        } catch (Exception e) {
            log.error("get from kudu fail", e);
            throw new RuntimeException("get from kudu fail", e);
        }


    }

    private class GetListRow implements Callback<Deferred<List<Row>>, RowResultIterator> {
        private final Cache<Row, List<Row>> cache;
        private final List<Row> rowList;
        private final AsyncKuduScanner asyncKuduScanner;
        private final CompletableFuture<Collection<Row>> future;
        private final Row keyRow;

        public GetListRow(Cache<Row, List<Row>> cache, List<Row> rowList, AsyncKuduScanner asyncKuduScanner, CompletableFuture<Collection<Row>> future, Row keyRow) {
            this.cache = cache;
            this.rowList = rowList;
            this.asyncKuduScanner = asyncKuduScanner;
            this.future = future;
            this.keyRow = keyRow;
        }

        @Override
        public Deferred<List<Row>> call(RowResultIterator results) {
            for (RowResult result : results) {
                rowList.add(toFlinkRow(result));
            };
            if (asyncKuduScanner.hasMoreRows()) {
                return asyncKuduScanner.nextRows().addCallbackDeferring(this);
            }
            if (cache != null) {
                cache.put(keyRow, rowList);
            }
            future.complete(rowList);
            return null;
        }

    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {

            client = new AsyncKuduClient.AsyncKuduClientBuilder(readerConfig.getMasters()).build();
            table = client.syncClient().openTable(tableInfo.getName());
            session = client.newSession();
        } catch (Exception e) {
            throw new Exception("build redis async client fail", e);
        }

        try {
            //初始化缓存大小
            this.cache = cacheMaxSize <= 0 || cacheExpireMs <= 0 ? null : CacheBuilder.newBuilder()
                    .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(cacheMaxSize)
                    .build();
            log.info("cache is null ?:{} cacheMaxSize: {},cacheExpireMs: {}", cache == null, cacheMaxSize, cacheExpireMs);
        } catch (Exception e) {
            throw new Exception("build cache fail", e);

        }
    }


    //返回类型
    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }


    //扫尾工作，关闭连接
    @Override
    public void close() throws IOException {
        if (cache != null) {
            cache.cleanUp();
        }
        ;
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            log.error("Error while closing session.", e);
        }
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            log.error("Error while closing client.", e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private KuduReaderConfig readerConfig;
        private KuduTableInfo tableInfo;
        private List<KuduFilterInfo> tableFilters;
        private TableSchema flinkSchema;
        private String[] keyNames;
        private List<String> tableProjections;

        public Builder setTableProjections(List<String> tableProjections) {
            this.tableProjections = tableProjections;
            return this;
        }

        public Builder setKeyNames(String[] keyNames) {
            this.keyNames = keyNames;
            return this;
        }


        public Builder setReaderConfig(KuduReaderConfig readerConfig) {
            this.readerConfig = readerConfig;
            return this;
        }

        public Builder setTableInfo(KuduTableInfo tableInfo) {
            this.tableInfo = tableInfo;
            return this;
        }

        public Builder setTableFilters(List<KuduFilterInfo> tableFilters) {
            this.tableFilters = tableFilters;
            return this;
        }


        public Builder setFlinkSchema(TableSchema flinkSchema) {
            this.flinkSchema = flinkSchema;
            return this;
        }

        public KuduLookupFunction build() {
            KuduLookupFunction kuduLookupFunction = new KuduLookupFunction(readerConfig, tableInfo, flinkSchema, tableFilters, tableProjections, keyNames);
            return kuduLookupFunction;
        }
    }
}
