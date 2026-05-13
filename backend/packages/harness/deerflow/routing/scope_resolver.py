"""Skill scope resolver for SkillRouter.

Computes the base allowed skill set (frontend ∩ registry) and the final
scope after optional SkillRouter filtering.  Centralises logic that was
previously scattered across the middleware, gateway, and frontend.
"""


class SkillScopeResolver:
    """Calculate base_scope and final_scope for skill injection."""

    @staticmethod
    def resolve_base_scope(
        *,
        frontend_enabled_skill_ids: list[str] | None,
    ) -> list[str]:
        """Calculate the base allowed skill set.

        Parameters
        ----------
        frontend_enabled_skill_ids :
            - ``None``  → use all registry-enabled skills.
            - ``[]``    → user disabled all skills → return empty list.
            - ``["a","b"]`` → intersection with registry-enabled skills.

        Returns
        -------
        list[str]
            Sorted list of skill IDs in the base scope.
        """
        from deerflow.skills import load_skills

        registry_skills = load_skills(enabled_only=True)
        registry_ids = {s.name for s in registry_skills}

        if frontend_enabled_skill_ids is None:
            return sorted(registry_ids)

        frontend_set = set(frontend_enabled_skill_ids)
        return sorted(registry_ids & frontend_set)

    @staticmethod
    def resolve_final_scope(
        *,
        skill_router_enabled: bool,
        base_scope_ids: list[str],
        routed_skill_ids: list[str] | None = None,
    ) -> list[str]:
        """Calculate final available skills.

        Parameters
        ----------
        skill_router_enabled :
            Whether the SkillRouter middleware is active.
        base_scope_ids :
            Output of ``resolve_base_scope()``.
        routed_skill_ids :
            Skill IDs returned by the router (ignored when router is off).

        Returns
        -------
        list[str]
            Final skill list for prompt injection and tool whitelisting.
        """
        if not skill_router_enabled:
            return base_scope_ids

        if not routed_skill_ids:
            return []

        base_set = set(base_scope_ids)
        return [sid for sid in routed_skill_ids if sid in base_set]
