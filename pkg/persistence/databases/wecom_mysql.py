from __future__ import annotations

import sqlalchemy.ext.asyncio as sqlalchemy_asyncio
from sqlalchemy import text

from .. import database


@database.manager_class('wecom_mysql')
class WecomMySQLDatabaseManager(database.BaseDatabaseManager):
    """企业微信调度器专用MySQL数据库管理器"""

    async def initialize(self) -> None:
        # 从配置读取MySQL连接信息
        mysql_config = self.ap.cfg_mgr.data.get('wecom_scheduler_db', {})
        
        host = mysql_config.get('host', 'localhost')
        port = mysql_config.get('port', 3306)
        username = mysql_config.get('username', 'root')
        password = mysql_config.get('password', '')
        database_name = mysql_config.get('database', 'wecom_scheduler')
        
        connection_string = f"mysql+aiomysql://{username}:{password}@{host}:{port}/{database_name}"
        
        self.engine = sqlalchemy_asyncio.create_async_engine(
            connection_string,
            echo=False,
            pool_size=20,
            max_overflow=30,
            pool_timeout=30,
            pool_recycle=3600
        )
        
        # 初始化数据库表
        await self._create_tables()
    
    async def _create_tables(self):
        """创建企业微信调度器所需的表"""
        
        # cursor状态表
        cursor_table_sql = """
        CREATE TABLE IF NOT EXISTS wecom_cursor (
            open_kfid VARCHAR(64) PRIMARY KEY,
            cursor TEXT NOT NULL DEFAULT '',
            last_sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            INDEX idx_last_sync_time (last_sync_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        
        # 消息状态表 - 基于企业微信真实API字段结构，支持1分钟内多次重试
        message_status_table_sql = """
        CREATE TABLE IF NOT EXISTS wecom_message_status (
            msgid VARCHAR(64) PRIMARY KEY,
            open_kfid VARCHAR(64) NOT NULL,
            external_userid VARCHAR(64),
            callback_id VARCHAR(64) NOT NULL,
            msgtype VARCHAR(32) NOT NULL,
            send_time TIMESTAMP NOT NULL,
            status TINYINT NOT NULL DEFAULT 0 COMMENT '0:待处理 1:处理中 2:已完成 3:最终失败 4:重试中',
            retry_count TINYINT DEFAULT 0,
            first_failed_at TIMESTAMP NULL COMMENT '首次失败时间',
            next_retry_at TIMESTAMP NULL COMMENT '下次重试时间',
            error_msg TEXT,
            raw_message_data JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            INDEX idx_open_kfid_time (open_kfid, send_time),
            INDEX idx_external_userid (external_userid),
            INDEX idx_msgtype (msgtype),
            INDEX idx_status (status),
            INDEX idx_next_retry_at (next_retry_at),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        
        # 已处理消息表
        processed_messages_table_sql = """
        CREATE TABLE IF NOT EXISTS wecom_processed_messages (
            msgid VARCHAR(64) PRIMARY KEY,
            open_kfid VARCHAR(64) NOT NULL,
            external_userid VARCHAR(64),
            msgtype VARCHAR(32) NOT NULL,
            send_time TIMESTAMP NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            batch_id VARCHAR(64) NOT NULL,
            
            INDEX idx_open_kfid_time (open_kfid, send_time),
            INDEX idx_external_userid (external_userid),
            INDEX idx_batch_id (batch_id),
            INDEX idx_processed_at (processed_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        
        async with self.engine.begin() as conn:
            await conn.execute(text(cursor_table_sql))
            await conn.execute(text(message_status_table_sql))
            await conn.execute(text(processed_messages_table_sql))
    
    async def get_cursor(self, open_kfid: str) -> str:
        """获取cursor"""
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("SELECT cursor FROM wecom_cursor WHERE open_kfid = :open_kfid"),
                {"open_kfid": open_kfid}
            )
            row = result.fetchone()
            return row[0] if row else ""
    
    async def update_cursor(self, open_kfid: str, cursor: str):
        """更新cursor"""
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO wecom_cursor (open_kfid, cursor, last_sync_time, updated_at) 
                    VALUES (:open_kfid, :cursor, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE 
                        cursor = :cursor, 
                        last_sync_time = NOW(), 
                        updated_at = NOW()
                """),
                {"open_kfid": open_kfid, "cursor": cursor}
            )
    
    async def is_message_processed(self, msgid: str) -> bool:
        """检查消息是否已处理"""
        async with self.engine.begin() as conn:
            # 先检查热数据
            result = await conn.execute(
                text("SELECT status FROM wecom_message_status WHERE msgid = :msgid"),
                {"msgid": msgid}
            )
            row = result.fetchone()
            if row:
                return row[0] == 2  # 状态为已完成
            
            # 再检查已处理表
            result = await conn.execute(
                text("SELECT 1 FROM wecom_processed_messages WHERE msgid = :msgid"),
                {"msgid": msgid}
            )
            return result.fetchone() is not None
    
    async def insert_message_status(self, msgid: str, open_kfid: str, callback_id: str, raw_data: dict):
        """插入消息状态"""
        import json
        from datetime import datetime
        
        async with self.engine.begin() as conn:
            # 基于企业微信真实API字段
            send_time = datetime.fromtimestamp(raw_data.get('send_time', 0))
            external_userid = raw_data.get('external_userid', '')
            msgtype = raw_data.get('msgtype', 'text')
            
            await conn.execute(
                text("""
                    INSERT INTO wecom_message_status 
                    (msgid, open_kfid, external_userid, callback_id, msgtype, send_time, status, raw_message_data)
                    VALUES (:msgid, :open_kfid, :external_userid, :callback_id, :msgtype, :send_time, 0, :raw_message_data)
                    ON DUPLICATE KEY UPDATE updated_at = NOW()
                """),
                {
                    "msgid": msgid,
                    "open_kfid": open_kfid,
                    "external_userid": external_userid,
                    "callback_id": callback_id,
                    "msgtype": msgtype,
                    "send_time": send_time,
                    "raw_message_data": json.dumps(raw_data)
                }
            )
    
    async def mark_message_completed(self, msgid: str):
        """标记消息处理完成"""
        async with self.engine.begin() as conn:
            # 更新状态为已完成
            await conn.execute(
                text("""
                    UPDATE wecom_message_status 
                    SET status = 2, updated_at = NOW()
                    WHERE msgid = :msgid
                """),
                {"msgid": msgid}
            )
            
            # 移动到已处理表
            await conn.execute(
                text("""
                    INSERT INTO wecom_processed_messages 
                    (msgid, open_kfid, external_userid, msgtype, send_time, batch_id)
                    SELECT msgid, open_kfid, external_userid, msgtype, send_time, callback_id
                    FROM wecom_message_status 
                    WHERE msgid = :msgid AND status = 2
                    ON DUPLICATE KEY UPDATE processed_at = NOW()
                """),
                {"msgid": msgid}
            )
            
            # 删除热数据记录
            await conn.execute(
                text("DELETE FROM wecom_message_status WHERE msgid = :msgid"),
                {"msgid": msgid}
            )
    
    async def handle_message_failure(self, msgid: str, error_msg: str) -> tuple[int, int]:
        """处理消息失败，1分钟内多次重试策略
        
        Returns:
            tuple[int, int]: (status, delay_seconds)
            status: -1=最终失败, 0=安排重试
            delay_seconds: 重试延迟秒数
        """
        async with self.engine.begin() as conn:
            # 获取当前状态和重试信息
            result = await conn.execute(
                text("""
                    SELECT retry_count, first_failed_at, status 
                    FROM wecom_message_status 
                    WHERE msgid = :msgid
                """),
                {"msgid": msgid}
            )
            row = result.fetchone()
            
            if not row:
                return (-1, 0)
            
            retry_count, first_failed_at, status = row
            
            # 如果是首次失败，记录失败时间
            if first_failed_at is None:
                from datetime import datetime
                now = datetime.now()
                delay_seconds = 15  # 15秒后首次重试
                
                await conn.execute(
                    text("""
                        UPDATE wecom_message_status 
                        SET status = 4, retry_count = 0, first_failed_at = :first_failed_at,
                            error_msg = :error_msg, updated_at = NOW()
                        WHERE msgid = :msgid
                    """),
                    {
                        "first_failed_at": now,
                        "error_msg": error_msg, 
                        "msgid": msgid
                    }
                )
                
                return (0, delay_seconds)  # 表示已安排重试，15秒延迟
            else:
                # 检查是否超过1分钟窗口
                from datetime import datetime, timedelta
                now = datetime.now()
                retry_window_end = first_failed_at + timedelta(minutes=1)
                
                if now > retry_window_end or retry_count >= 3:
                    # 超过1分钟窗口或重试次数上限，标记为最终失败
                    await conn.execute(
                        text("""
                            UPDATE wecom_message_status 
                            SET status = 3, error_msg = :error_msg, updated_at = NOW()
                            WHERE msgid = :msgid
                        """),
                        {"error_msg": f"重试超时或次数上限: {error_msg}", "msgid": msgid}
                    )
                    
                    return (-1, 0)  # 表示最终失败
                else:
                    # 计算下次重试延迟：15秒、30秒、45秒
                    retry_delays = [15, 30, 45]
                    delay_seconds = retry_delays[min(retry_count, 2)]
                    
                    await conn.execute(
                        text("""
                            UPDATE wecom_message_status 
                            SET retry_count = retry_count + 1, error_msg = :error_msg, updated_at = NOW()
                            WHERE msgid = :msgid
                        """),
                        {"error_msg": error_msg, "msgid": msgid}
                    )
                    
                    return (0, delay_seconds)  # 表示已安排重试
    
    async def get_failed_messages(self, limit: int = 100) -> list:
        """获取失败消息列表，用于分析"""
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("""
                    SELECT msgid, open_kfid, external_userid, msgtype, send_time, 
                           error_msg, created_at, updated_at
                    FROM wecom_message_status 
                    WHERE status = 3 
                    ORDER BY updated_at DESC
                    LIMIT :limit
                """),
                {"limit": limit}
            )
            
            return [dict(row._mapping) for row in result.fetchall()]
    
    async def get_messages_to_retry(self, limit: int = 50) -> list:
        """获取需要重试的消息列表"""
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("""
                    SELECT msgid, open_kfid, external_userid, msgtype, send_time, 
                           retry_count, first_failed_at, error_msg, raw_message_data
                    FROM wecom_message_status 
                    WHERE status = 4 AND next_retry_at <= NOW()
                    ORDER BY next_retry_at ASC
                    LIMIT :limit
                """),
                {"limit": limit}
            )
            
            return [dict(row._mapping) for row in result.fetchall()]
    
    async def mark_message_retrying(self, msgid: str) -> bool:
        """标记消息为重试中状态"""
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("""
                    UPDATE wecom_message_status 
                    SET status = 1, updated_at = NOW()
                    WHERE msgid = :msgid AND status = 4
                """),
                {"msgid": msgid}
            )
            
            return result.rowcount > 0
    
    async def reset_failed_message(self, msgid: str) -> bool:
        """重新设置失败消息为待处理状态（手动恢复）"""
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("""
                    UPDATE wecom_message_status 
                    SET status = 0, retry_count = 0, first_failed_at = NULL, 
                        next_retry_at = NULL, error_msg = NULL, updated_at = NOW()
                    WHERE msgid = :msgid AND status = 3
                """),
                {"msgid": msgid}
            )
            
            return result.rowcount > 0