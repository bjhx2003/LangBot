from __future__ import annotations

import asyncio
import json
import time
import traceback
import uuid
import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING

from quart import request
from kafka import KafkaProducer, KafkaConsumer
from kafka.partitioner import RoundRobinPartitionAssignor
from sqlalchemy import text

if TYPE_CHECKING:
    from libs.wecom_customer_service_api.api import WecomCSClient
    from libs.wecom_customer_service_api.wecomcsevent import WecomCSEvent
    from pkg.persistence.databases.wecom_mysql import WecomMySQLDatabaseManager
    from ..logger import EventLogger


class WecomCallbackScheduler:
    """企业微信回调调度器 - 替换现有回调处理逻辑"""
    
    def __init__(self, kafka_producer: KafkaProducer, logger: 'EventLogger'):
        self.kafka_producer = kafka_producer
        self.logger = logger
    
    async def handle_callback_request(self, wecom_client: 'WecomCSClient'):
        """处理企业微信回调 - 替换WecomCSClient.handle_callback_request"""
        try:
            # 复用现有的验证和解密逻辑
            from libs.wecom_api.WXBizMsgCrypt3 import WXBizMsgCrypt
            
            msg_signature = request.args.get('msg_signature')
            timestamp = request.args.get('timestamp')
            nonce = request.args.get('nonce')
            
            wxcpt = WXBizMsgCrypt(wecom_client.token, wecom_client.aes, wecom_client.corpid)
            
            if request.method == 'GET':
                # 复用现有验证逻辑
                echostr = request.args.get('echostr')
                ret, reply_echo_str = wxcpt.VerifyURL(msg_signature, timestamp, nonce, echostr)
                if ret != 0:
                    raise Exception(f'验证失败，错误码: {ret}')
                return reply_echo_str
                
            elif request.method == 'POST':
                # 解密消息
                encrypt_msg = await request.data
                ret, xml_msg = wxcpt.DecryptMsg(encrypt_msg, msg_signature, timestamp, nonce)
                if ret != 0:
                    raise Exception(f'消息解密失败，错误码: {ret}')
                
                # 🔧 立即发送触发消息到Kafka，不阻塞回调
                await self._send_pull_trigger(xml_msg)
                
                # 🔧 立即返回success
                return 'success'
                
        except Exception as e:
            await self.logger.error(f"回调处理失败: {e}")
            return 'success'  # 即使出错也返回success，避免企业微信重发
    
    async def _send_pull_trigger(self, xml_msg: str):
        """发送拉取触发消息"""
        try:
            # 解析回调数据
            root = ET.fromstring(xml_msg)
            callback_data = {
                'token': root.find('Token').text,
                'open_kfid': root.find('OpenKfId').text,
                'callback_id': str(uuid.uuid4()),
                'timestamp': int(time.time())
            }
            
            # 🔧 使用open_kfid作为分区键，确保同一客服的消息串行处理
            await asyncio.to_thread(
                self.kafka_producer.send,
                'wecom.pull.trigger',
                key=callback_data['open_kfid'].encode(),
                value=json.dumps(callback_data).encode()
            )
            
            await self.logger.debug(f"发送拉取触发消息: {callback_data['open_kfid']}")
            
        except Exception as e:
            await self.logger.error(f"发送Kafka触发消息失败: {e}")


class WecomMessagePuller:
    """消息拉取器 - 复用现有API和业务逻辑"""
    
    def __init__(self, wecom_client: 'WecomCSClient', db_manager: 'WecomMySQLDatabaseManager', logger: 'EventLogger'):
        self.wecom_client = wecom_client  # 复用现有客户端
        self.db_manager = db_manager
        self.logger = logger
        self._running = False
    
    async def start_consuming(self):
        """开始消费Kafka触发消息"""
        self._running = True
        
        consumer = KafkaConsumer(
            'wecom.pull.trigger',
            bootstrap_servers=['localhost:9092'],
            group_id='wecom-puller-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8'),
            # 🔧 每个分区只有一个消费者，确保串行处理
            partition_assignment_strategy=[RoundRobinPartitionAssignor],
            max_poll_records=1
        )
        
        await self.logger.info("企业微信消息拉取器启动")
        
        try:
            while self._running:
                message_batch = consumer.poll(timeout_ms=1000)
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        await self._handle_pull_trigger(message.value)
                        consumer.commit_async()
        except Exception as e:
            await self.logger.error(f"消费Kafka消息失败: {e}")
        finally:
            consumer.close()
    
    def stop_consuming(self):
        """停止消费"""
        self._running = False
    
    async def _handle_pull_trigger(self, trigger_data: dict):
        """处理拉取触发消息"""
        open_kfid = trigger_data['open_kfid']
        token = trigger_data['token']
        callback_id = trigger_data['callback_id']
        
        try:
            await self.logger.debug(f"开始处理拉取触发: {open_kfid}")
            
            # Step 1: 获取当前cursor
            current_cursor = await self.db_manager.get_cursor(open_kfid)
            
            # Step 2: 调用sync_msg API拉取消息（复用现有逻辑）
            messages = await self._pull_all_messages(open_kfid, token, current_cursor)
            
            if not messages:
                await self.logger.debug(f"没有新消息: {open_kfid}")
                return
            
            # Step 3: 串行处理所有消息
            await self._process_messages_serially(messages, callback_id)
            
            # Step 4: 更新cursor（基于API返回的next_cursor）
            if messages and hasattr(self, '_last_next_cursor') and self._last_next_cursor:
                await self.db_manager.update_cursor(open_kfid, self._last_next_cursor)
                await self.logger.debug(f"更新cursor: {open_kfid} -> {self._last_next_cursor}")
            
        except Exception as e:
            await self.logger.error(f"拉取处理失败: {open_kfid}, {e}")
            await self.logger.error(traceback.format_exc())
    
    async def _pull_all_messages(self, open_kfid: str, token: str, cursor: str = None) -> list:
        """拉取所有消息 - 修改现有get_detailed_message_list逻辑"""
        import httpx
        
        all_messages = []
        current_cursor = cursor
        self._last_next_cursor = None  # 保存最后的next_cursor用于更新
        
        while True:
            # 🔧 复用现有的API调用逻辑，但修改参数处理
            if not await self.wecom_client.check_access_token():
                self.wecom_client.access_token = await self.wecom_client.get_access_token(
                    self.wecom_client.secret
                )
            
            url = f"{self.wecom_client.base_url}/kf/sync_msg?access_token={self.wecom_client.access_token}"
            
            params = {
                'token': token,
                'voice_format': 0,
                'open_kfid': open_kfid,
                'limit': 1000,
            }
            
            # 🔧 正确使用cursor
            if current_cursor:
                params['cursor'] = current_cursor
            
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=params)
                data = response.json()
                
                # 🔧 复用现有的token刷新逻辑
                if data.get('errcode') in [40014, 42001]:
                    self.wecom_client.access_token = await self.wecom_client.get_access_token(
                        self.wecom_client.secret
                    )
                    continue
                
                if data.get('errcode') != 0:
                    raise Exception(f"sync_msg API失败: {data}")
                
                msg_list = data.get('msg_list', [])
                if not msg_list:
                    break
                
                # 🔧 处理所有消息，而不只是最后一条
                for msg_data in msg_list:
                    # 🔧 复用现有的图片处理逻辑
                    if msg_data.get('msgtype') == 'image':
                        media_id = msg_data.get('image', {}).get('media_id')
                        if media_id:
                            msg_data['picurl'] = await self.wecom_client.get_pic_url(media_id)
                    
                    # 🔧 确保包含企业微信API的标准字段
                    msg_data['open_kfid'] = open_kfid
                    # msgid, msgtype, external_userid, send_time 等字段已在API响应中
                    all_messages.append(msg_data)
                
                # 🔧 保存最后的next_cursor
                current_cursor = data.get('next_cursor')
                has_more = data.get('has_more', False)
                
                if current_cursor:
                    self._last_next_cursor = current_cursor
                
                if not has_more or not current_cursor:
                    break
        
        await self.logger.info(f"拉取到 {len(all_messages)} 条消息: {open_kfid}")
        return all_messages
    
    async def _process_messages_serially(self, messages: list, callback_id: str):
        """串行处理消息列表 - 复用现有业务逻辑"""
        processed_count = 0
        failed_count = 0
        
        for msg_data in messages:
            try:
                msgid = msg_data['msgid']  # 🔧 使用企业微信API的msgid字段
                open_kfid = msg_data['open_kfid']
                
                # 🔧 幂等性检查
                if await self.db_manager.is_message_processed(msgid):
                    await self.logger.debug(f"消息已处理，跳过: {msgid}")
                    continue
                
                # 🔧 插入消息状态记录
                await self.db_manager.insert_message_status(msgid, open_kfid, callback_id, msg_data)
                
                # 🔧 处理单条消息，复用现有业务逻辑
                await self._process_single_message(msg_data)
                
                # 🔧 标记处理完成
                await self.db_manager.mark_message_completed(msgid)
                processed_count += 1
                
            except Exception as e:
                await self.logger.error(f"处理消息失败: {msg_data.get('msgid')}, {e}")
                await self._handle_message_failure(msg_data, str(e))
                failed_count += 1
        
        await self.logger.info(f"批次处理完成: {callback_id}, 成功: {processed_count}, 失败: {failed_count}")
    
    async def _process_single_message(self, msg_data: dict):
        """处理单条消息 - 复用现有的WecomCSClient._handle_message逻辑"""
        try:
            # 🔧 复用现有的事件转换逻辑
            from libs.wecom_customer_service_api.wecomcsevent import WecomCSEvent
            
            event = WecomCSEvent.from_payload(msg_data)
            if not event:
                raise Exception("无法构造WecomCSEvent对象")
            
            # 🔧 复用现有的消息处理逻辑
            await self.wecom_client._handle_message(event)
            
        except Exception as e:
            await self.logger.error(f"业务逻辑处理失败: {msg_data.get('msgid')}, {e}")
            raise
    
    async def _handle_message_failure(self, msg_data: dict, error_msg: str):
        """处理消息失败 - 1分钟内多次重试策略（基于MQ延迟消息）"""
        msgid = msg_data.get('msgid')  # 🔧 使用企业微信API的msgid字段
        if not msgid:
            return
        
        retry_status, delay_seconds = await self.db_manager.handle_message_failure(msgid, error_msg)
        
        if retry_status == -1:
            await self.logger.error(f"消息最终失败: {msgid}, 错误: {error_msg}")
        elif retry_status == 0:
            # 🔧 发送延迟重试消息到MQ
            await self._send_retry_message(msg_data, delay_seconds)
            await self.logger.warning(f"消息处理失败，已安排{delay_seconds}秒后重试: {msgid}, 错误: {error_msg}")
    
    async def _send_retry_message(self, msg_data: dict, delay_seconds: int):
        """发送延迟重试消息到MQ"""
        try:
            import time
            
            retry_data = {
                'action': 'retry_message',
                'msgid': msg_data.get('msgid'),
                'msg_data': msg_data,
                'retry_at': int(time.time()) + delay_seconds,
                'delay_seconds': delay_seconds
            }
            
            # 🔧 发送到重试队列，使用msgid作为分区键保证顺序
            await asyncio.to_thread(
                self.kafka_producer.send,
                'wecom.message.retry',
                key=msg_data.get('msgid').encode(),
                value=json.dumps(retry_data).encode()
            )
            
        except Exception as e:
            await self.logger.error(f"发送重试消息失败: {msg_data.get('msgid')}, {e}")


class WecomRetryManager:
    """重试管理器 - 基于MQ延迟消息的重试策略和记录清理"""
    
    def __init__(self, db_manager: 'WecomMySQLDatabaseManager', logger: 'EventLogger'):
        self.db_manager = db_manager
        self.logger = logger
        self._running = False
        self._message_processor = None
    
    def set_message_processor(self, processor_func):
        """设置消息处理函数（用于重试）"""
        self._message_processor = processor_func
    
    async def start_retry_consumer(self):
        """启动重试消费者 - 消费MQ中的延迟重试消息"""
        self._running = True
        
        consumer = KafkaConsumer(
            'wecom.message.retry',
            bootstrap_servers=['localhost:9092'],
            group_id='wecom-retry-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8'),
            auto_offset_reset='latest'
        )
        
        await self.logger.info("企业微信重试消费者启动")
        
        try:
            while self._running:
                message_batch = consumer.poll(timeout_ms=1000)
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        await self._handle_retry_message(message.value)
                        consumer.commit_async()
        except Exception as e:
            await self.logger.error(f"重试消费者错误: {e}")
        finally:
            consumer.close()
    
    async def start_cleanup_scanner(self):
        """启动清理扫描器 - 定期清理过期记录"""
        self._running = True
        
        await self.logger.info("企业微信清理扫描器启动")
        
        while self._running:
            try:
                await self._cleanup_old_records()
                await asyncio.sleep(3600)  # 每小时清理一次
            except Exception as e:
                await self.logger.error(f"清理扫描器错误: {e}")
                await asyncio.sleep(3600)
    
    def stop_scanners(self):
        """停止所有组件"""
        self._running = False
    
    async def _handle_retry_message(self, retry_data: dict):
        """处理MQ中的重试消息"""
        try:
            import time
            
            # 检查是否到了重试时间
            retry_at = retry_data.get('retry_at', 0)
            current_time = int(time.time())
            
            if current_time < retry_at:
                # 还没到重试时间，重新放回队列
                await self.logger.debug(f"重试消息还未到期，等待 {retry_at - current_time} 秒")
                await asyncio.sleep(min(retry_at - current_time, 60))  # 最多等待60秒
                return
            
            msgid = retry_data.get('msgid')
            msg_data = retry_data.get('msg_data')
            
            await self.logger.info(f"开始处理重试消息: {msgid}")
            
            # 🔧 标记为重试中，避免并发
            if not await self.db_manager.mark_message_retrying(msgid):
                await self.logger.warning(f"消息可能已被其他进程处理: {msgid}")
                return
            
            # 🔧 调用消息处理器（复用现有业务逻辑）
            if self._message_processor:
                await self._message_processor(msg_data)
                
                # 🔧 重试成功，标记为已完成
                await self.db_manager.mark_message_completed(msgid)
                await self.logger.info(f"消息重试成功: {msgid}")
            else:
                raise Exception("消息处理器未设置")
                
        except Exception as e:
            # 🔧 重试失败，继续按重试策略处理
            msgid = retry_data.get('msgid', 'unknown')
            await self.logger.error(f"消息重试失败: {msgid}, 错误: {e}")
            
            # 重新调用失败处理逻辑（可能触发下一次重试或最终失败）
            if retry_data.get('msg_data'):
                msg_data = retry_data['msg_data']
                # 这里需要获取消息拉取器实例来调用_handle_message_failure
                # 或者在数据库中直接更新失败状态
                await self.db_manager.handle_message_failure(msgid, str(e))
    
    async def _cleanup_old_records(self):
        """清理过期记录"""
        async with self.db_manager.engine.begin() as conn:
            # 清理7天前的已完成消息
            await conn.execute(
                text("""
                    DELETE FROM wecom_processed_messages 
                    WHERE processed_at < DATE_SUB(NOW(), INTERVAL 7 DAY)
                """)
            )
            
            # 清理30天前的失败消息（保留更久用于分析）
            result = await conn.execute(
                text("""
                    DELETE FROM wecom_message_status 
                    WHERE status = 3 AND created_at < DATE_SUB(NOW(), INTERVAL 30 DAY)
                """)
            )
            
            if result.rowcount > 0:
                await self.logger.info(f"清理了 {result.rowcount} 条过期失败消息")