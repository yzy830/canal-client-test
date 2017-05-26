package com.gerald.client.test.slave;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.gerald.client.test.slave.CanalConsumer.CanalConsumerBuilder;


public class Slave {    
    public static final int MYSQL_DUPLICATE_PK = 1062;
    
    private static final Logger logger = LoggerFactory.getLogger(Slave.class);
    
    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
        CanalConsumerBuilder builder = new CanalConsumerBuilder();
        
        Connection connection = MySqlUtils.getConnection();
        
        connection.setAutoCommit(false);
        
        CanalConsumer canalConsumer = builder.setAddress(new InetSocketAddress("192.168.1.100", 11111))
                                             .setDestination("example")
               .setConsumer((m) -> {
                   try {
                       connection.setAutoCommit(false);
                       for(Entry entry : m.getEntries()) {
                           if(entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                               continue;
                           }
                           
                           try {
                               RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
                               
                               System.out.println("event type = " + rowChange.getEventType());
                               System.out.println("sql = " + rowChange.getSql());
                               
                               for(RowData row : rowChange.getRowDatasList()) {
                                   System.out.println("-----------------------------------");
                                   for(Column column : row.getAfterColumnsList()) {
                                       System.out.println(column.getName() + " = " + column.getValue() + ", sql type = " + column.getSqlType());
                                   }
                                   System.out.println("-----------------------------------");
                               }
                               
                               execute(entry, rowChange, connection);
                           } catch (Exception e) {
                               throw new RuntimeException(e);
                           }
                     }
                     connection.commit();
                } catch (Exception e1) {
                    try {
                        connection.rollback();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    throw new RuntimeException(e1);
                }
                   
               })
               .build();
        
        canalConsumer.start();
        
        System.in.read();
        
        try {
            canalConsumer.stop();
            
            connection.close();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private static void execute(Entry entry, RowChange rowChange, Connection conn) {
        if(rowChange.getIsDdl()) {
            return;
        }
        
        if(rowChange.getEventType() == EventType.INSERT) {
            insert(entry, rowChange, conn);
        } else if(rowChange.getEventType() == EventType.DELETE) {
            delete(entry, rowChange, conn);
        } else if(rowChange.getEventType() == EventType.UPDATE) {
            update(entry, rowChange, conn);
        }
    }
    
    private static void insert(Entry entry, RowChange rowChange, Connection conn) {
        if(rowChange.getRowDatasCount() <= 0) {
            logger.info("entry row with type <INSERT>");
            return;
        }
        
        String schemaName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        for(RowData row : rowChange.getRowDatasList()) {
            if(row.getAfterColumnsCount() <= 0) {
                continue;
            }
            
            StringBuilder builder = new StringBuilder().append("insert into ")
                    .append(schemaName + "." + tableName + " ")
                    .append("(");
            
            for(Column column : row.getAfterColumnsList()) {
                builder.append(column.getName()).append(",");
            }
            builder.replace(builder.length() - 1, builder.length(), ") ");
            
            builder.append(" values (");
            List<Object> values = new ArrayList<>();
            for(Column column : row.getAfterColumnsList()) {
                builder.append("?").append(",");
                values.add(column.getValue());
            }
            builder.replace(builder.length() - 1, builder.length(), ") ");
            
            PreparedStatement s = null;
            try {
                s = conn.prepareStatement(builder.toString());
                int index = 1;
                for(Object value : values) {
                    s.setObject(index++, value);
                }
                int r = s.executeUpdate();
            } catch (SQLException e) {
                if(e.getErrorCode() == MYSQL_DUPLICATE_PK) {
                    logger.warn("ingore " + s + " for duplicated primary key");
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    
    private static void delete(Entry entry, RowChange rowChange, Connection conn) {
        if(rowChange.getRowDatasCount() <= 0) {
            logger.info("entry row with type <INSERT>");
            return;
        }
        
        String schemaName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        for(RowData row : rowChange.getRowDatasList()) {
            if(row.getBeforeColumnsCount() <= 0) {
                continue;
            }
            
            StringBuilder builder = new StringBuilder().append("delete FROM ")
                    .append(schemaName + "." + tableName + " ")
                    .append(" where ");
            
            Object value = null;
            for(Column column : row.getBeforeColumnsList()) {
                if(column.getIsKey()) {
                    builder.append(column.getName()).append(" = ").append("?");
                    value = column.getValue();
                }
            }
            
            PreparedStatement s = null;
            try {
                s = conn.prepareStatement(builder.toString());
                s.setObject(1, value);
                int r = s.executeUpdate();
                if(r < 1) {
                    logger.warn("ignore delete for key = " + value);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private static void update(Entry entry, RowChange rowChange, Connection conn) {
        if(rowChange.getRowDatasCount() <= 0) {
            logger.info("entry row with type <INSERT>");
            return;
        }
        
        String schemaName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        for(RowData row : rowChange.getRowDatasList()) {
            if(row.getAfterColumnsCount() <= 0) {
                continue;
            }
            
            StringBuilder builder = new StringBuilder().append("update ")
                    .append(schemaName + "." + tableName + " ")
                    .append("set ");
           
            List<Object> values = new ArrayList<>();
            Object key = null;
            String keyName = null;
            for(Column column : row.getAfterColumnsList()) {
                if(!column.getIsKey()) {
                    builder.append(column.getName()).append(" = ?").append(",");
                    values.add(column.getValue());
                } else {
                    key = column.getValue();
                    keyName = column.getName();
                }
            }
            builder.replace(builder.length() - 1, builder.length(), "");
            builder.append(" where ").append(keyName).append(" = ?");
            
            PreparedStatement s = null;
            try {
                s = conn.prepareStatement(builder.toString());
                int index = 1;
                for(Object value : values) {
                    s.setObject(index++, value);
                }
                s.setObject(index, key);
                int r = s.executeUpdate();
                if(r < 1) {
                    logger.warn("ignore update for key = " + key);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
