"""Source payload -> ``DetectionUnit`` converters, one per gate."""

from deerflow.compliance.normalizers.base import Normalizer, build_unit, flatten_fields, make_text_items, stringify
from deerflow.compliance.normalizers.llm_output import LlmOutputNormalizer, extract_text
from deerflow.compliance.normalizers.skill_result import SkillResultNormalizer, try_parse_skill_result
from deerflow.compliance.normalizers.user_input import UploadedFileNormalizer, UserInputNormalizer

__all__ = [
    "LlmOutputNormalizer",
    "Normalizer",
    "SkillResultNormalizer",
    "UploadedFileNormalizer",
    "UserInputNormalizer",
    "build_unit",
    "extract_text",
    "flatten_fields",
    "make_text_items",
    "stringify",
    "try_parse_skill_result",
]
