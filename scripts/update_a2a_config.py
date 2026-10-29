#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 更新A2A配置到数据库的脚本

import os
import sys
import tomli
import logging
import datetime
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# 导入数据库模型
from onecityagent.models.db_models import DB

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("update_a2a_config")

def read_toml_config(config_path):
    """读取TOML配置文件"""
    try:
        with open(config_path, "rb") as f:
            config = tomli.load(f)
        return config
    except Exception as e:
        logger.error(f"读取配置文件失败: {str(e)}")
        return None

def update_a2a_config_to_db():
    """将A2A配置更新到数据库"""
    # 配置文件路径
    config_path = project_root / "config" / "config.toml"
    
    # 读取配置
    config = read_toml_config(config_path)
    if not config or "a2a" not in config:
        logger.error("配置文件中未找到a2a配置")
        return False
    
    a2a_config = config["a2a"]
    
    # 获取数据库连接
    db = DB.get_instance()
    
    try:
        # 使用数据库连接上下文
        with db.connection_context():
            # 准备A2A配置数据
            a2a_agents = [
                {
                    "agent_id": "llm-qa",
                    "agent_name": "问答智能体",
                    "agent_url": a2a_config.get("llm_qa", ""),
                    "status": 1,
                    "type": 1,
                    "description": "大模型问答智能体",
                    "agent_type": 4
                },
                {
                    "agent_id": "intent",
                    "agent_name": "意图识别智能体",
                    "agent_url": a2a_config.get("intent", ""),
                    "status": 1,
                    "type": 1,
                    "description": "意图识别智能体",
                    "agent_type": 0
                },
                {
                    "agent_id": "planning",
                    "agent_name": "规划智能体",
                    "agent_url": a2a_config.get("planner", ""),
                    "status": 1,
                    "type": 1,
                    "description": "A2A规划智能体",
                    "agent_type": 1
                },
                {
                    "agent_id": "thinking",
                    "agent_name": "反馈智能体",
                    "agent_url": a2a_config.get("thinker", ""),
                    "status": 1,
                    "type": 1,
                    "description": "内置的反馈智能体",
                    "agent_type": 3
                },
                {
                    "agent_id": "acting",
                    "agent_name": "执行智能体",
                    "agent_url": a2a_config.get("actor", ""),
                    "status": 1,
                    "type": 1,
                    "description": "A2A协议智能体，支持跨智能体通信和协作",
                    "agent_type": 2
                }
            ]
            
            # 获取当前时间
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 对每个智能体配置进行更新
            for agent in a2a_agents:
                # 检查智能体是否已存在
                cursor = db.execute_sql(
                    "SELECT agent_id FROM a2a_agents WHERE agent_id = %s",
                    (agent["agent_id"],)
                )
                exists = cursor.fetchone()
                cursor.close()
                
                if exists:
                    # 更新已存在的记录
                    db.execute_sql(
                        """
                        UPDATE a2a_agents 
                        SET agent_url = %s, update_date = %s 
                        WHERE agent_id = %s
                        """,
                        (agent["agent_url"], now, agent["agent_id"])
                    )
                    logger.info(f"已更新智能体配置: {agent['agent_id']} -> {agent['agent_url']}")
                else:
                    # 插入新记录
                    db.execute_sql(
                        """
                        INSERT INTO a2a_agents 
                        (agent_id, agent_name, agent_url, status, type, create_date, update_date, description, agent_type) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            agent["agent_id"], 
                            agent["agent_name"], 
                            agent["agent_url"], 
                            agent["status"], 
                            agent["type"], 
                            now, 
                            now, 
                            agent["description"], 
                            agent["agent_type"]
                        )
                    )
                    logger.info(f"已添加智能体配置: {agent['agent_id']} -> {agent['agent_url']}")
            
            logger.info("A2A配置已成功更新到数据库")
            return True
            
    except Exception as e:
        logger.error(f"更新A2A配置到数据库失败: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("开始更新A2A配置到数据库...")
    success = update_a2a_config_to_db()
    if success:
        logger.info("A2A配置更新完成")
    else:
        logger.error("A2A配置更新失败")
        sys.exit(1)