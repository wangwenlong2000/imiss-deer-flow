"""Runtime routing structures produced by SkillRouterMiddleware."""

from pydantic import BaseModel


class SelectedSkill(BaseModel):
    id: str
    role: str  # "primary" | "supporting" | "fallback"
    score: float
    reason: str | None = None


class SceneTask(BaseModel):
    scene_task_id: str
    segment_id: str
    segment_text: str
    scene: str | None = None
    input_refs: list[str] = []
    task_types: list[str] = []
    selected_skills: list[SelectedSkill] = []
    expected_outputs: list[str] = []
    depends_on: list[str] = []


class RoutingContext(BaseModel):
    route_mode: str  # "none" | "single_segment" | "multi_segment"
    trigger: bool
    primary_goal: str | None = None
    scene_tasks: list[SceneTask] = []
    global_selected_skills: list[str] = []
    global_allowed_tools: list[str] = []
    confidence: float = 0.0
    route_reason: str | None = None
