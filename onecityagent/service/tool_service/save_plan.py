import ast
import re
import uvicorn
from datetime import datetime

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


from onecityagent.config import Config
from onecityagent.models.db_models import PlanData, DB
from onecityagent.utils.common_utils import datetime_format
from onecityagent.logger import logger


class SavePlanReq(BaseModel):
    username: str
    scene: str
    qa_pairs: str


# 移除了CreatePlanReq和UpdatePlanReq，因为对应的FastAPI接口已移除


class PlanDataService:
    """计划数据服务类"""
    model = PlanData
    
    @staticmethod
    def parse_plan_to_list(plan_text):
        """将数据库中的plan字符串解析成列表格式"""
        if not plan_text:
            return []
        
        # 使用正则表达式提取<|begin_step|>和<|end_step|>之间的内容
        pattern = r'<\|begin_step\|>(.*?)<\|end_step\|>'
        matches = re.findall(pattern, plan_text, re.DOTALL)
        return [match.strip() for match in matches]
    
    @staticmethod
    def format_list_to_plan(plan_list):
        """将列表格式转换成数据库存储的字符串格式"""
        if not plan_list:
            return ""
        
        return "".join(
            f"<|begin_step|>{step.strip()}<|end_step|>"
            for step in plan_list
        ).rstrip()
    
    @staticmethod
    def format_plan_dict(plan_dict):
        """格式化计划字典，将plan字段转换为列表"""
        if plan_dict and 'plan' in plan_dict:
            plan_dict['plan'] = PlanDataService.parse_plan_to_list(plan_dict['plan'])
        return plan_dict
    
    @classmethod
    def save(cls, **kwargs):
        """保存数据"""
        try:
            with DB.connection_context():
                instance = cls.model.create(**kwargs)
                return instance
        except Exception as e:
            logger.exception(f"Save plan data error: {str(e)}")
            return False
    
    @classmethod
    def create_plan(cls, scene, plan, user_id, tenant_id, owner=None):
        """创建新的计划数据
        
        Args:
            scene: 场景名称
            plan: 计划内容，可以是字符串或列表格式
            user_id: 用户ID
            tenant_id: 租户ID
            owner: 所有者
        """
        try:
            # 如果plan是列表，转换为数据库存储格式
            if isinstance(plan, list):
                plan = cls.format_list_to_plan(plan)
            
            # 生成基于时间的ID
            plan_id = PlanData.generate_time_based_id()
            
            current_time = datetime_format(datetime.now())
            plan_data = {
                "id": plan_id,  # 使用生成的时间ID
                "scene": scene,
                "owner": owner or "用户",
                "plan": plan,
                "user_id": user_id,
                "tenant_id": tenant_id,
                "status": "1",
                "create_date": current_time,
                "update_date": current_time
            }
            return cls.save(**plan_data)
        except Exception as e:
            logger.exception(f"Create plan error: {str(e)}")
            return False
    
    @classmethod
    def update_plan(cls, plan_id, **update_data):
        """更新计划数据
        
        Args:
            plan_id: 计划ID
            **update_data: 更新的数据，plan字段可以是字符串或列表格式
        """
        try:
            # 如果更新数据中包含plan字段且为列表，转换为数据库存储格式
            if 'plan' in update_data and isinstance(update_data['plan'], list):
                update_data['plan'] = cls.format_list_to_plan(update_data['plan'])
            
            update_data['update_date'] = datetime.now()
            with DB.connection_context():
                cls.model.update(**update_data).where(cls.model.id == plan_id).execute()
                return True
        except Exception as e:
            logger.exception(f"Update plan error: {str(e)}")
            return False
    
    @classmethod
    def delete_plan(cls, plan_id):
        """删除计划数据（物理删除）"""
        try:
            with DB.connection_context():
                query = cls.model.delete().where(cls.model.id == plan_id)
                return query.execute() > 0
        except Exception as e:
            logger.exception(f"Delete plan error: {str(e)}")
            return False
    
    @classmethod
    def get_plan_by_id(cls, plan_id):
        """根据ID获取计划数据，返回列表格式的plan字段"""
        try:
            with DB.connection_context():
                plan = cls.model.get(
                    cls.model.id == plan_id,
                    cls.model.status == '1'
                )
                plan_dict = plan.to_dict()
                return cls.format_plan_dict(plan_dict)
        except cls.model.DoesNotExist:
            return None
        except Exception as e:
            logger.exception(f"Get plan by id error: {str(e)}")
            return None
    
    @classmethod
    def get_plans_by_user(cls, user_id, tenant_id=None, scene=None, page=1, page_size=20):
        """根据用户ID查询计划列表"""
        try:
            with DB.connection_context():
                conditions = [
                    cls.model.user_id == user_id,
                    cls.model.status == '1'
                ]
                
                if tenant_id:
                    conditions.append(cls.model.tenant_id == tenant_id)
                
                if scene:
                    conditions.append(cls.model.scene.contains(scene))
                
                # 计算偏移量
                offset = (page - 1) * page_size
                
                # 查询数据
                query = cls.model.select().where(*conditions).order_by(cls.model.create_date.desc())
                total = query.count()
                plans = list(query.offset(offset).limit(page_size))
                
                return {
                    'total': total,
                    'page': page,
                    'page_size': page_size,
                    'data': [cls.format_plan_dict(plan.to_dict()) for plan in plans]
                }
        except Exception as e:
            logger.exception(f"Get plans by user error: {str(e)}")
            return {
                'total': 0,
                'page': page,
                'page_size': page_size,
                'data': []
            }

    @classmethod
    def count_person_plans_by_user(cls, user_id, tenant_id=None):
        """统计某用户的个人(owner='person')方案数量"""
        try:
            with DB.connection_context():
                conditions = [
                    cls.model.user_id == user_id,
                    cls.model.owner == 'person',
                    cls.model.status == '1'
                ]
                if tenant_id:
                    conditions.append(cls.model.tenant_id == tenant_id)
                return cls.model.select().where(*conditions).count()
        except Exception as e:
            logger.exception(f"Count person plans by user error: {str(e)}")
            return 0
    
    @classmethod
    def get_plan_by_scene_and_user(cls, scene, user_id):
        """根据场景和用户ID获取特定计划，返回列表格式的plan字段"""
        try:
            with DB.connection_context():
                plan = cls.model.get(
                    cls.model.scene == scene,
                    cls.model.user_id == user_id,
                    cls.model.status == '1'
                )
                plan_dict = plan.to_dict()
                return cls.format_plan_dict(plan_dict)
        except cls.model.DoesNotExist:
            return None
        except Exception as e:
            logger.exception(f"Get plan by scene and user error: {str(e)}")
            return None
            
    @classmethod
    def get_plan_id_by_scene_and_user(cls, scene, user_id):
        """根据场景和用户ID查询计划，如果存在则返回数据ID，否则返回空"""
        try:
            with DB.connection_context():
                plan = cls.model.get(
                    cls.model.scene == scene,
                    cls.model.user_id == user_id,
                    cls.model.status == '1'
                )
                return plan.id
        except cls.model.DoesNotExist:
            return None
        except Exception as e:
            logger.exception(f"Get plan id by scene and user error: {str(e)}")
            return None
    
    @classmethod
    def search_plans(cls, user_id, keyword, tenant_id=None, page=1, page_size=20):
        """根据关键词搜索用户的计划"""
        try:
            with DB.connection_context():
                conditions = [
                    cls.model.user_id == user_id,
                    cls.model.status == '1'
                ]
                
                if tenant_id:
                    conditions.append(cls.model.tenant_id == tenant_id)
                
                # 添加关键词搜索条件（在场景或计划内容中搜索）
                if keyword:
                    keyword_condition = (
                        cls.model.scene.contains(keyword) |
                        cls.model.plan.contains(keyword)
                    )
                    conditions.append(keyword_condition)
                
                # 计算偏移量
                offset = (page - 1) * page_size
                
                # 查询数据
                query = cls.model.select().where(*conditions).order_by(cls.model.create_date.desc())
                total = query.count()
                plans = list(query.offset(offset).limit(page_size))
                
                return {
                    'total': total,
                    'page': page,
                    'page_size': page_size,
                    'keyword': keyword,
                    'data': [cls.format_plan_dict(plan.to_dict()) for plan in plans]
                }
        except Exception as e:
            logger.exception(f"Search plans error: {str(e)}")
            return {
                'total': 0,
                'page': page,
                'page_size': page_size,
                'keyword': keyword,
                'data': []
            }
    
    @classmethod
    def create_default_plans(cls, user_id, tenant_id):
        """为新用户创建默认计划数据"""
        current_time = datetime_format(datetime.now())
        default_plans = [
            {
                "scene": "洪涝灾害告警",
                "owner": "public",
                "plan": "<|begin_step|>从信息中分析出洪涝地点经纬度，持续的时间，灾情涉及范围等信息，输出灾情研判简报，控制在200字以内<|end_step|><|begin_step|>输出洪涝处置方案<|end_step|><|begin_step|>调用工具，将灾情通知相关部门处置<|end_step|>",
                "user_id": user_id,
                "tenant_id": tenant_id,
                "status": "1",
                "create_date": current_time,
                "update_date": current_time
            },
            {
                "scene": "消防告警",
                "owner": "public",
                "plan": "<|begin_step|>从信息中分析出地点经纬度，时间，涉及范围，输出事件研判简报，控制在200字以内<|end_step|><|begin_step|>输出事件处置方案<|end_step|><|begin_step|>调用工具，消防安全相关信息通知相关部门处置<|end_step|>",
                "user_id": user_id,
                "tenant_id": tenant_id,
                "status": "1",
                "create_date": current_time,
                "update_date": current_time
            }
        ]
        
        try:
            with DB.connection_context():
                for plan_data in default_plans:
                    cls.model.create(**plan_data)
                return True
        except Exception as e:
            logger.exception(f"Create default plans error: {str(e)}")
            return False


app = FastAPI()

# 数据库配置
config = Config()
db_config = config.database_config
db_cfg = {
    "host": db_config.host,
    "port": db_config.port,
    "user": db_config.user,
    "password": db_config.password,
    "database": db_config.name
}


@app.post("/save_plan")
def save_plan(req: SavePlanReq):
    """
    保存或更新用户方案：
    1. 从 qa_pairs 中提取"规划智能体"最后一次输出的字符串列表
    2. 转成 <|begin_step|>...<|end_step|> 格式
    3. 写入/更新 plan_database(scene,plan,owner,created_at,updated_at)
    """
    try:
        # 提取规划智能体的输出
        plan_steps = re.search(r"规划智能体：\s*(\[.*?\])", req.qa_pairs, re.DOTALL)
        if not plan_steps:
            raise ValueError("未找到规划智能体输出")
        try:
            plan_steps = ast.literal_eval(plan_steps.group(1))
        except Exception:
            raise ValueError("规划智能体输出格式不合法")

        # 使用统一的格式转换方法组装 plan 字段
        plan_text = PlanDataService.format_list_to_plan(plan_steps)

        # 保存用户方案到知识库中
        saved_flag = PlanDataService.create_plan(req.scene.strip(), plan_text, req.username, None)
        if not saved_flag:
            raise HTTPException(status_code=500, detail="保存用户方案失败")
        return {"status": "ok", "scene": req.scene}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

