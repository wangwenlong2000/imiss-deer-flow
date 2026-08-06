import re
from pathlib import Path

from .types import Skill


def _parse_front_matter(front_matter: str) -> dict[str, str]:
    """Parse the simple SKILL.md frontmatter fields used by the UI.

    This intentionally stays small, but supports YAML block scalars such as
    ``description: >-`` and ``description: |`` because many skills use them.
    """
    metadata: dict[str, str] = {}
    lines = front_matter.splitlines()
    i = 0
    while i < len(lines):
        raw_line = lines[i]
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or ":" not in stripped:
            i += 1
            continue

        key, value = stripped.split(":", 1)
        key = key.strip()
        value = value.strip()

        if value in {">", ">-", ">+", "|", "|-", "|+"}:
            block_lines: list[str] = []
            i += 1
            while i < len(lines):
                next_line = lines[i]
                if next_line.strip() and not next_line.startswith((" ", "\t")):
                    break
                block_lines.append(next_line.strip())
                i += 1
            parts = [part for part in block_lines if part]
            if value.startswith("|"):
                metadata[key] = "\n".join(parts)
            else:
                metadata[key] = " ".join(parts)
            continue

        metadata[key] = value.strip('"').strip("'")
        i += 1

    return metadata


def parse_skill_file(skill_file: Path, category: str, relative_path: Path | None = None) -> Skill | None:
    """
    Parse a SKILL.md file and extract metadata.

    Args:
        skill_file: Path to the SKILL.md file
        category: Category of the skill ('public' or 'custom')

    Returns:
        Skill object if parsing succeeds, None otherwise
    """
    if not skill_file.exists() or skill_file.name != "SKILL.md":
        return None

    try:
        content = skill_file.read_text(encoding="utf-8")

        # Extract YAML front matter
        # Pattern: ---\nkey: value\n---
        front_matter_match = re.match(r"^---\s*\n(.*?)\n---\s*\n", content, re.DOTALL)

        if not front_matter_match:
            return None

        front_matter = front_matter_match.group(1)
        metadata = _parse_front_matter(front_matter)

        # Extract required fields
        name = metadata.get("name")
        description = metadata.get("description")

        if not name or not description:
            return None

        license_text = metadata.get("license")

        return Skill(
            name=name,
            description=description,
            license=license_text,
            skill_dir=skill_file.parent,
            skill_file=skill_file,
            relative_path=relative_path or Path(skill_file.parent.name),
            category=category,
            enabled=True,  # Default to enabled, actual state comes from config file
        )

    except Exception as e:
        print(f"Error parsing skill file {skill_file}: {e}")
        return None
