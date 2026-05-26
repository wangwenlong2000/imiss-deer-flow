import os
from pathlib import Path

from .parser import parse_skill_file
from .types import Skill


def get_skills_root_path() -> Path:
    """
    Get the root path of the skills directory.

    Returns:
        Path to the skills directory (deer-flow/skills)
    """
    # loader.py lives at packages/harness/deerflow/skills/loader.py — 5 parents up reaches backend/
    backend_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
    # skills directory is sibling to backend directory
    skills_dir = backend_dir.parent / "skills"
    return skills_dir


def _dedupe_skills_by_name(skills: list[Skill]) -> list[Skill]:
    """Keep one Skill per name, preferring canonical shallow paths.

    Custom skills are sometimes copied into nested helper directories such as
    ``skills/custom/custom/...`` or another skill's subdirectory. The frontend
    and skill enable API both address skills by name, so duplicate names are
    ambiguous and produce duplicate React keys.
    """
    selected: dict[str, Skill] = {}

    def sort_key(skill: Skill) -> tuple[int, str, str]:
        return (
            len(skill.relative_path.parts),
            skill.category,
            skill.relative_path.as_posix(),
        )

    for skill in skills:
        existing = selected.get(skill.name)
        if existing is None or sort_key(skill) < sort_key(existing):
            selected[skill.name] = skill

    return list(selected.values())


def load_skills(
    skills_path: Path | None = None,
    use_config: bool = True,
    enabled_only: bool = False,
    available_skills: set[str] | None = None,
    categories: set[str] | None = None,
) -> list[Skill]:
    """
    Load all skills from the skills directory.

    Scans both public and custom skill directories, parsing SKILL.md files
    to extract metadata. The enabled state is determined by the skills_state_config.json file.

    Args:
        skills_path: Optional custom path to skills directory.
                     If not provided and use_config is True, uses path from config.
                     Otherwise defaults to deer-flow/skills
        use_config: Whether to load skills path from config (default: True)
        enabled_only: If True, only return enabled skills (default: False)
        available_skills: If provided, only return skills whose names are in this set.
                         If None, return all (enabled) skills.
        categories: If provided, only scan skills in these top-level categories
                    (for example {"public"} or {"custom"}).

    Returns:
        List of Skill objects, sorted by name
    """
    if skills_path is None:
        if use_config:
            try:
                from deerflow.config import get_app_config

                config = get_app_config()
                skills_path = config.skills.get_skills_path()
            except Exception:
                # Fallback to default if config fails
                skills_path = get_skills_root_path()
        else:
            skills_path = get_skills_root_path()

    if not skills_path.exists():
        return []

    skills = []

    scan_categories = ["public", "custom"]
    if categories is not None:
        scan_categories = [category for category in scan_categories if category in categories]

    # Scan public and custom directories
    for category in scan_categories:
        category_path = skills_path / category
        if not category_path.exists() or not category_path.is_dir():
            continue

        for current_root, dir_names, file_names in os.walk(category_path):
            # Keep traversal deterministic and skip hidden directories.
            dir_names[:] = sorted(name for name in dir_names if not name.startswith("."))
            if "SKILL.md" not in file_names:
                continue

            skill_file = Path(current_root) / "SKILL.md"
            relative_path = skill_file.parent.relative_to(category_path)

            skill = parse_skill_file(skill_file, category=category, relative_path=relative_path)
            if skill:
                skills.append(skill)

    skills = _dedupe_skills_by_name(skills)

    # Load skills state configuration and update enabled status
    # NOTE: We use ExtensionsConfig.from_file() instead of get_extensions_config()
    # to always read the latest configuration from disk. This ensures that changes
    # made through the Gateway API (which runs in a separate process) are immediately
    # reflected in the LangGraph Server when loading skills.
    try:
        from deerflow.config.extensions_config import ExtensionsConfig

        extensions_config = ExtensionsConfig.from_file()
        for skill in skills:
            skill.enabled = extensions_config.is_skill_enabled(skill.name, skill.category)
    except Exception as e:
        # If config loading fails, default to all enabled
        print(f"Warning: Failed to load extensions config: {e}")

    # Filter by enabled status if requested
    if enabled_only:
        skills = [skill for skill in skills if skill.enabled]

    # Filter by available skills if requested
    if available_skills is not None:
        skills = [skill for skill in skills if skill.name in available_skills]

    # Sort by name for consistent ordering
    skills.sort(key=lambda s: s.name)

    return skills


def load_public_skills(
    skills_path: Path | None = None,
    use_config: bool = True,
    enabled_only: bool = False,
    available_skills: set[str] | None = None,
) -> list[Skill]:
    """Load public skills only."""
    return load_skills(
        skills_path=skills_path,
        use_config=use_config,
        enabled_only=enabled_only,
        available_skills=available_skills,
        categories={"public"},
    )


def load_custom_skills(
    skills_path: Path | None = None,
    use_config: bool = True,
    enabled_only: bool = False,
    available_skills: set[str] | None = None,
) -> list[Skill]:
    """Load custom skills only."""
    return load_skills(
        skills_path=skills_path,
        use_config=use_config,
        enabled_only=enabled_only,
        available_skills=available_skills,
        categories={"custom"},
    )
