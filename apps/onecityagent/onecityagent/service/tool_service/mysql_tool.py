import pymysql
from loguru import logger
from typing import Optional, List, Dict, Any


def get_db_connection(db_config: Dict[str, Any]) -> pymysql.connections.Connection:
    """
    创建并返回数据库连接（使用上下文管理器自动关闭）

    Args:
        db_config: 数据库连接配置（需包含host, port, user, password, database）

    Returns:
        pymysql连接对象
    """
    try:
        conn = pymysql.connect(
            host=db_config.get("host", "localhost"),
            port=int(db_config.get("port", 3306)),
            user=db_config.get("user"),
            password=db_config.get("password"),
            database=db_config.get("database"),
            cursorclass=pymysql.cursors.DictCursor
        )
        logger.info("数据库连接成功")
        return conn
    except Exception as e:
        logger.error(f"数据库连接失败: {str(e)}")
        raise  # 向上抛出异常，由调用者处理


def query_plan_by_scene(scene: str, db_config: Dict[str, Any], user_id: str, owner: str) -> Optional[str]:
    """
    通过场景名称查询对应的处置方案（优化版）

    Args:
        scene: 场景名称（如'火灾'，'洪涝'）
        db_config: 数据库连接配置（需包含host, port, user, password, database）
        user_id:  当前查询用户的名称

    Returns:
        方案内容（字符串）或None（未找到/出错）
    """
    try:
        with get_db_connection(db_config) as conn:  # 自动关闭连接
            with conn.cursor() as cursor:  # 自动关闭游标
                query = "SELECT plan FROM plan_data WHERE scene = %s and owner = %s and user_id = %s and status = 1;"
                cursor.execute(query, (scene,owner, user_id))
                result = cursor.fetchone()
                return result["plan"] if result else None
    except Exception as e:
        logger.error(f"查询场景方案失败（场景：{scene}）: {str(e)}")
        return None


def query_user_scenes(db_config: Dict[str, Any], user_id: str) -> List[str]:
    """
    根据用户名从从表中查询所有已保存的用户请求（修正功能逻辑）

    Args:
        db_config: 数据库连接配置（需包含host, port, user, password, database）
        user_id:  当前用户的名称，用于过滤 user_id 字段

    Returns:
        用户请求列表（字符串列表），无数据时返回空列表
    """
    try:
        with get_db_connection(db_config) as conn:
            with conn.cursor() as cursor:
                # 查询所有场景请求，过滤owner为'person'的记录
                query_sql = "SELECT scene FROM plan_data WHERE owner = 'person' and user_id = %s and status = 1;"
                cursor.execute(query_sql, user_id)
                results = cursor.fetchall()
                # 提取所有request字段，过滤空值
                return [row["scene"].strip() for row in results if row.get("scene")]
    except Exception as e:
        logger.error(f"查询用户 {user_id} 的请求失败: {str(e)}")
        return []

def query_a2a_agent(db_config: Dict[str, Any], agent_type: int) -> List[Dict[str, str]]:
    """
    查询筛选后的所有智能体数据

    Args:
        db_config: 数据库连接配置
        agent_type: 智能体类型

    Returns:
        包含所有智能体信息的列表，每个智能体为一个字典
    """
    try:
        with get_db_connection(db_config) as conn:
            with conn.cursor() as cursor:
                query_sql = "SELECT agent_name, agent_url FROM a2a_agents WHERE status=1 and agent_type=%s ;"
                cursor.execute(query_sql, (agent_type,))
                results = cursor.fetchall()
                return [
                    {row["agent_name"]:row["agent_url"]}
                    for row in results
                    if row.get("agent_name") and row.get("agent_url")
                ]
    except Exception as e:
        logger.error(f"查询智能体 {agent_type} 的请求失败: {str(e)}")
        return []


