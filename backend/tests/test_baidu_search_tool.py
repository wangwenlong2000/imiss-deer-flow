from deerflow.community.baidu.tools import _BaiduSearchParser


def test_baidu_search_parser_extracts_results():
    parser = _BaiduSearchParser()
    parser.feed(
        """
        <div class="result c-container">
          <h3><a href="https://www.baidu.com/link?url=abc">示例 标题</a></h3>
          <span>这是摘要内容。</span>
        </div>
        <div class="result">
          <a href="https://example.com/page">第二个结果</a>
          <div>第二段摘要</div>
        </div>
        """
    )

    assert parser.results == [
        {
            "title": "示例 标题",
            "url": "https://www.baidu.com/link?url=abc",
            "snippet": "这是摘要内容。",
        },
        {
            "title": "第二个结果",
            "url": "https://example.com/page",
            "snippet": "第二段摘要",
        },
    ]
