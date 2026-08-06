import json
from html.parser import HTMLParser
from urllib.parse import quote_plus

import httpx
from langchain.tools import tool

from deerflow.config import get_app_config


class _BaiduSearchParser(HTMLParser):
    """Extract coarse organic search results from Baidu HTML."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.results: list[dict[str, str]] = []
        self._depth = 0
        self._in_result = False
        self._in_anchor = False
        self._skip_text = False
        self._current: dict[str, str] | None = None
        self._title_parts: list[str] = []
        self._text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = dict(attrs)
        class_name = attr.get("class") or ""
        if tag == "div" and not self._in_result and self._is_result_container(class_name):
            self._in_result = True
            self._depth = 1
            self._current = {"title": "", "url": "", "snippet": ""}
            self._title_parts = []
            self._text_parts = []
            return

        if self._in_result:
            if tag == "div":
                self._depth += 1
            if tag in {"script", "style"}:
                self._skip_text = True
            if tag == "a" and self._current is not None and not self._current["url"]:
                href = attr.get("href") or ""
                if href.startswith("http"):
                    self._current["url"] = href
                    self._in_anchor = True

    def handle_endtag(self, tag: str) -> None:
        if not self._in_result:
            return

        if tag == "a":
            self._in_anchor = False
        if tag in {"script", "style"}:
            self._skip_text = False
        if tag == "div":
            self._depth -= 1
            if self._depth <= 0:
                self._finish_result()

    def handle_data(self, data: str) -> None:
        if not self._in_result or self._skip_text:
            return

        text = " ".join(data.split())
        if not text:
            return
        if self._in_anchor:
            self._title_parts.append(text)
        else:
            self._text_parts.append(text)

    @staticmethod
    def _is_result_container(class_name: str) -> bool:
        classes = set(class_name.split())
        return bool(classes & {"result", "c-container"}) or "result-op" in classes

    def _finish_result(self) -> None:
        if self._current is not None:
            title = " ".join(self._title_parts).strip()
            snippet = " ".join(self._text_parts).strip()
            url = self._current["url"].strip()
            if title and url:
                self.results.append({
                    "title": title,
                    "url": url,
                    "snippet": snippet[:500],
                })
        self._in_result = False
        self._in_anchor = False
        self._skip_text = False
        self._current = None
        self._title_parts = []
        self._text_parts = []
        self._depth = 0


def _search_baidu(query: str, *, max_results: int, timeout: float, user_agent: str) -> list[dict[str, str]]:
    url = f"https://www.baidu.com/s?wd={quote_plus(query)}&rn={max_results}"
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.6",
    }
    with httpx.Client(timeout=timeout, follow_redirects=True, headers=headers) as client:
        response = client.get(url)
        response.raise_for_status()

    parser = _BaiduSearchParser()
    parser.feed(response.text)
    return parser.results[:max_results]


@tool("web_search", parse_docstring=True)
def web_search_tool(query: str) -> str:
    """Search the web with Baidu.

    Args:
        query: The query to search for.
    """
    config = get_app_config().get_tool_config("web_search")
    max_results = 5
    timeout = 10.0
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
    if config is not None:
        max_results = int(config.model_extra.get("max_results", max_results))
        timeout = float(config.model_extra.get("timeout", timeout))
        user_agent = str(config.model_extra.get("user_agent", user_agent))

    try:
        results = _search_baidu(
            query,
            max_results=max_results,
            timeout=timeout,
            user_agent=user_agent,
        )
    except Exception as exc:
        return f"Error: Baidu search failed: {exc}"

    return json.dumps(results, indent=2, ensure_ascii=False)
