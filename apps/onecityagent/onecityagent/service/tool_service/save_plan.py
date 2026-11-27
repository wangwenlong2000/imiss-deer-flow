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


from onecityagent.service.plan_data import PlanDataService


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

