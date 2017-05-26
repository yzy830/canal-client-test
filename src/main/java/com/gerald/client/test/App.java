package com.gerald.client.test;

import java.net.InetSocketAddress;
import java.util.List;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;

public class App {
    public static void main( String[] args ) {
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("192.168.1.100", 11111), "example", "", "");
        
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            
            int totalEmptyCount = 100000;
            
            while(emptyCount < totalEmptyCount) {
                Message message = connector.getWithoutAck(batchSize);
                
                
                long batchId = message.getId();
                int size = message.getEntries().size();
                
                if(batchId == -1 || size == 0) {
                    emptyCount++;
                    System.out.println("empty count: " + emptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch(InterruptedException e) {
                        
                    }
                } else {
                    emptyCount = 0;
                    printEntry(message.getEntries());
                    
                    connector.ack(batchId);
                }
            }
        } finally {
            connector.disconnect();
        }
    }
    
    private static void printEntry(List<Entry> entries) {
        for(Entry entry : entries) {
            if(entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }
            
            RowChange rowChange = null;
            
            try {
                rowChange = RowChange.parseFrom(entry.getStoreValue());
            } catch(Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString());
            }
            
            EventType eventType = rowChange.getEventType();
            
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s, serverId = %d",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType, entry.getHeader().getServerId()));
            
            for(RowData rowData : rowChange.getRowDatasList()) {
                if(eventType == EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                } else if(eventType == EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                } else {
                    System.out.println("------------->before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("------------->after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }
    
    private static void printColumn(List<Column> columns) {
        for(Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + ", update = " + column.getUpdated());
        }
    }
}
