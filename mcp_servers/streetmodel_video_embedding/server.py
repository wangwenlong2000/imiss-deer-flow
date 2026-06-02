from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from .core import (
    DEFAULT_OWNER,
    DEFAULT_TARGET_INDEX,
    streetmodel_embed_query as embed_query_core,
    streetmodel_embed_video as embed_video_core,
    streetmodel_health as health_core,
)

mcp = FastMCP("streetmodel-video-embedding")


@mcp.tool()
def streetmodel_health() -> dict[str, Any]:
    """Check StreetModel video embedding service health and model availability."""
    return health_core()


@mcp.tool()
def streetmodel_embed_video(
    video_uri: str,
    video_id: str | None = None,
    target_index: str = DEFAULT_TARGET_INDEX,
    owner: str = DEFAULT_OWNER,
    copy_video_to_shared: bool = False,
    allow_shared_index: bool = False,
    return_vector: bool = False,
    video_preprocess: str | None = None,
    frame_sample_fps: float | None = None,
    max_sampled_frames: int | None = None,
    proxy_output_fps: float | None = None,
    proxy_max_width: int | None = None,
) -> dict[str, Any]:
    """Generate a StreetModel video embedding for one video URI and write it to Elasticsearch."""
    return embed_video_core(
        video_uri=video_uri,
        video_id=video_id,
        target_index=target_index,
        owner=owner,
        copy_video_to_shared=copy_video_to_shared,
        allow_shared_index=allow_shared_index,
        return_vector=return_vector,
        video_preprocess=video_preprocess,
        frame_sample_fps=frame_sample_fps,
        max_sampled_frames=max_sampled_frames,
        proxy_output_fps=proxy_output_fps,
        proxy_max_width=proxy_max_width,
    )


@mcp.tool()
def streetmodel_embed_query(
    query: str,
    return_vector: bool = False,
    query_vector_output: str | None = None,
) -> dict[str, Any]:
    """Generate a StreetModel text query embedding for semantic video retrieval."""
    return embed_query_core(
        query=query,
        return_vector=return_vector,
        query_vector_output=query_vector_output,
    )


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
