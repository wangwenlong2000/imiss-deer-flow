import re
from itertools import product
from typing import Any, Dict, List, Sequence

from .base import BaseDetector


class TextIdDetector(BaseDetector):
    violation_type = "text_id"

    ROLE_WORDS = [
        "举报人",
        "投诉人",
        "信访人",
        "当事人",
        "联系人",
        "负责人",
        "法定代表人",
        "执法对象",
        "申请人",
        "主申请人",
        "受益人",
        "原告",
        "被告",
        "被处罚人",
        "责任人",
        "住户",
        "业主",
    ]
    EVENT_WORDS = [
        "投诉",
        "举报",
        "信访",
        "处罚",
        "调查",
        "案件",
        "反映",
        "公示",
        "公告",
        "低保",
        "保障性住房",
        "庭审",
        "法院",
        "违纪",
        "违法",
        "扰民",
        "回访",
        "纠纷",
        "整治",
        "复核",
        "审核",
        "仲裁",
        "争议",
        "核查",
        "处置",
        "听证",
        "复议",
        "评估",
        "许可",
        "抽检",
        "申诉",
        "安检",
        "备案",
        "救助",
        "督办",
        "权属",
        "入学",
    ]
    ADDRESS_WORDS = [
        "小区",
        "单元",
        "大厦",
        "花园",
        "住址",
        "地址",
        "楼",
        "室",
        "号",
    ]
    ORG_WORDS = [
        "公司",
        "有限公司",
        "集团",
        "住建局",
        "街道办",
        "执法大队",
        "物业",
        "办公室",
        "服务站",
        "服务科",
        "服务窗口",
        "专班",
        "中队",
    ]

    POSITION_WORDS = [
        "窗口审核员",
        "审核专员",
        "权益服务员",
        "案件承办人",
        "信息公开专员",
        "现场核验员",
        "养老评估员",
        "网格协调员",
        "食品抽检联络员",
        "复议联络员",
        "权属调查员",
        "复议文书管理员",
        "工单督办员",
        "救助受理员",
        "安全检查员",
        "医保复核员",
        "档案员",
        "联络员",
        "调查员",
        "督办员",
        "受理员",
        "检查员",
        "复核员",
        "服务员",
        "专员",
        "现场见证人",
        "网格员",
        "听证记录员",
        "执法辅助人员",
        "政策咨询负责人",
        "项目现场负责人",
        "项目负责人",
        "现场负责人",
        "负责人",
        "联系人",
        "承办人",
        "经办人",
        "审核员",
        "记录员",
        "协管员",
        "工作人员",
        "课程顾问",
        "维修班长",
        "护理员",
        "收费员",
        "社工",
        "队长",
        "见证人",
        "负责",
    ]

    # A conservative surname-based candidate recognizer is used only when a
    # candidate is followed by a grammatical relation such as “为/负责/的”.
    # It avoids treating every two-to-four-character Chinese phrase as a name.
    COMMON_SURNAMES = (
        "赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨朱秦尤许何吕施张孔曹严华金魏陶姜"
        "戚谢邹喻柏水窦章云苏潘葛奚范彭郎鲁韦昌马苗凤花方俞任袁柳酆鲍史"
        "唐费廉岑薛雷贺倪汤滕殷罗毕郝邬安常乐于时傅皮卞齐康伍余元卜顾"
        "孟平黄和穆萧尹姚邵湛汪祁毛禹狄米贝明臧计伏成戴谈宋茅庞熊纪舒"
        "屈项祝董梁杜阮蓝闵席季麻强贾路娄危江童颜郭梅盛林刁钟徐邱骆高"
        "夏蔡田樊胡凌霍虞万支柯昝管卢莫经房裘缪干解应宗丁宣贲邓郁单杭"
        "洪包诸左石崔吉龚程嵇邢裴陆荣翁荀羊甄曲封芮储靳汲邴糜松井段富"
        "巫乌焦巴弓牧隗山谷车侯宓蓬全郗班仰秋仲伊宫宁仇栾暴甘钭厉戎祖"
        "武符刘景詹束龙叶幸司韶郜黎蓟薄印宿白怀蒲台从鄂索咸籍赖卓蔺屠"
        "蒙池乔阴胥能苍双闻莘党翟谭贡劳逄姬申扶堵冉宰郦雍郤璩桑桂濮牛"
        "寿通边扈燕冀郏浦尚农温别庄晏柴瞿阎充慕连茹习宦艾鱼容向古易慎"
        "戈廖庾终暨居衡步都耿满弘匡国文寇广禄阙东欧殳沃利蔚越夔隆师巩"
        "厍聂晁勾敖融冷訾辛阚那简饶空曾毋沙乜养鞠须丰巢关蒯相查后荆红"
        "游竺权逯盖益桓公"
    )

    NAME = re.compile(r"(?:姓名[:：]?|叫|被拘留人是|当事人|举报人|投诉人|信访人|联系人|负责人|主申请人[:：]?|受益人中，?|原告[:：]?|被告[:：]?)(?:是|为)?[:：]?\s*([\u4e00-\u9fff]{2,4}|[\u4e00-\u9fff]{1,3}某{1,2})|[\u4e00-\u9fff]某")
    MASKED_ID = re.compile(r"(?<!\d)(?:\d{17}[0-9Xx]|\d{6,17}[Xx]{2,8}\d{0,4})(?!\d)")
    MASKED_PHONE = re.compile(r"\b1[3-9]\d{1,8}X{2,6}\d{0,4}\b|\b1[3-9]\d{9}\b", re.IGNORECASE)
    MOJIBAKE_HINT = re.compile(r"\b\d{6,17}X{2,8}\b|\b1[3-9]\d{2,8}X{2,6}\b|XX|X{2,}")
    ADDRESS_SHAPE = re.compile(r"(?:XX|[\u4e00-\u9fff]{1,8}).{0,12}(?:路|街|大厦|花园|新村).{0,12}(?:\d+|XX).{0,8}(?:号|楼|栋|单元|室|平房)|(?:小区|花园|新村).{0,8}(?:\d+|XX).{0,8}(?:号|楼|栋|单元|室|平房)")
    ADDRESS_LABEL = re.compile(
        r"(?:家庭住址|居住地址|登记住址|登记地址|注册地址|居住地|住址|家住|住在|居住在|办公地点|常驻)"
        r"[为：:]?\s*([^，。；;！？\n]{2,48})"
    )
    ADDRESS_NUMBERED = re.compile(
        r"[\u4e00-\u9fffA-Za-z]{1,16}(?:公寓|小区|家园|花园|新村|苑|里|巷|院|大厦|社区|转运站)"
        r".{0,10}(?:\d+|XX)(?:号院|号|栋|幢|座|楼|单元|门|室|层|平房)"
    )
    ORGANIZATION = re.compile(
        r"[A-Za-z0-9\u4e00-\u9fff“”\"]{2,32}?"
        r"(?:有限责任公司|有限公司|事业部|住房保障中心|行政审批局|应急管理局|生态环境执法大队|"
        r"交通运输执法支队|综合执法队|市场监管所|自然资源所|居委会|街道办事处|街道办|民政办|服务站|"
        r"执法大队|管理局|医院|法院|社区|物业|公司|集团|机构|中心|支队|执法队|外包队|养老站|"
        r"办公室|服务科|服务窗口|窗口|专班|中队|科)"
    )
    CASE_ID = re.compile(
        r"(?:案件编号|案号|通知书编号|处罚决定书编号|文号)[：:]?\s*[A-Za-z0-9\u4e00-\u9fff〔〕（）()\-]{4,40}"
        r"|[A-Za-z\u4e00-\u9fff]{0,12}(?:字)?〔\d{4}〕第?\d{1,8}号"
    )
    ALIAS = re.compile(r"(?:别名|昵称|常用昵称)[为：:]?[“\"「]?([\u4e00-\u9fff]{1,4})[”\"」]?")
    QUOTED_ALIAS = re.compile(
        r"(?:网格员|护理员|负责人|联系人|工作人员)[“\"「]([小阿][\u4e00-\u9fff]{1,2})[”\"」]"
    )
    NEGATIVE_CONTEXT = re.compile(
        r"虚构示例|演示样例|示例姓名|不对应真实|样例值为空|字段名称|签名栏为空|"
        r"已将个人姓名替换|姓名字段已.*替换|姓名.*统一替换|人员均以.*表示|"
        r"仅使用.*匿名称谓|均为.*代称|均以编号代替|无法定位到具体自然人|"
        r"不包含实际姓名|未出现完整姓名|未出现具体承办人的姓名|不披露.*具体|"
        r"不需要个人.*具体信息|"
        r"(?:未|不|没有)(?:包含|出现|列出|列明|披露|展示|写入).{0,36}"
        r"(?:姓名|自然人|个人|身份|联系方式|住址|承办人|投诉人|工作人员)|"
        r"(?:姓名|联系方式|住址|人员字段).{0,20}(?:为空|删除|替换|匿名化)|"
        r"(?:作|已作|已经作|已进行)匿名化处理"
    )

    PERSON_LABEL = re.compile(
        rf"(?:姓名|申请人|投诉人|举报人|信访人|联系人|负责人|当事人|原告|被告|主申请人|"
        rf"被处罚人|责任人|项目联系人说明)(?:是|为)?[：:]?\s*([{COMMON_SURNAMES}][\u4e00-\u9fff]{{1,2}})"
    )

    PERSON_CANDIDATE = re.compile(
        rf"([{COMMON_SURNAMES}][\u4e00-\u9fff]{{1,2}})"
        r"(?=为|是|，|、|（|\(|）|\)|“|”|\"|｜|\||；|;|。|的|作为|现任|担任|负责|承办|参与|"
        r"登记|居住|住在|在|跟进|完成|处理|审核|复核|标注|任职|就职|系|$)"
    )

    def detect(self, sample: Dict[str, Any]) -> Dict[str, Any]:
        text = self.build_text_view(sample)
        locations: List[Dict[str, Any]] = []
        rules: List[str] = []
        score = 0
        counts: Dict[str, int] = {}

        def add_signal(kind: str, points: int, matched_text: str, rule_id: str) -> None:
            nonlocal score
            if counts.get(kind, 0) == 0:
                score += points
            counts[kind] = counts.get(kind, 0) + 1
            locations.append(self.make_location("content_text", kind, matched_text[:120], rule_id))
            rules.append(rule_id)

        for word in self.ROLE_WORDS:
            if word in text:
                add_signal("role_keyword", 2, word, "role-keyword")
                break

        for word in self.EVENT_WORDS:
            if word in text:
                add_signal("event_keyword", 2, word, "event-keyword")
                break

        if any(word in text for word in ["拘留", "涉嫌", "看守所", "通知书"]):
            add_signal("legal_case_context", 3, "legal case context", "legal-case-context")

        name_match = self.NAME.search(text)
        if name_match:
            add_signal("person_name", 2, name_match.group(0), "person-name-context")

        id_match = self.MASKED_ID.search(text)
        if id_match:
            add_signal("id_number", 4, id_match.group(0), "masked-or-full-id-number")

        phone_match = self.MASKED_PHONE.search(text)
        if phone_match:
            add_signal("phone", 3, phone_match.group(0), "masked-or-full-phone")

        if any(word in text for word in self.ORG_WORDS):
            add_signal("organization", 2, "organization keyword", "organization-keyword")

        address_match = self.ADDRESS_SHAPE.search(text)
        if address_match:
            add_signal("detailed_address", 4, address_match.group(0), "detailed-address")

        # Many normalized policy samples are mojibake, but digits, X masks, and XX placeholders survive.
        # This fallback keeps the detector useful while still requiring multiple quasi-identifying signals.
        if self.MOJIBAKE_HINT.search(text) and ("XXXX" in text or len(re.findall(r"\d", text)) >= 8):
            add_signal("masked_identifier_context", 2, "masked identifier context", "mojibake-masked-identifier-context")

        entities = self._extract_entities(text)
        relationships = self._find_relationships(text, entities)
        if relationships:
            score = max(score, 7 + min(3, len(relationships) - 1))
            for relationship in relationships:
                rule_id = relationship["rule_id"]
                rules.append(rule_id)
                for entity in relationship["entities"]:
                    counts[entity["type"]] = counts.get(entity["type"], 0) + 1
                    locations.append(
                        self.make_location(
                            "content_text",
                            entity["type"],
                            entity["text"][:120],
                            rule_id,
                            entity["start"],
                            entity["end"],
                        )
                    )

        # Avoid treating a bare public organization or public place as a text identifier leak.
        if counts.get("id_number") and counts.get("phone"):
            score = max(score, 7)
        if counts.get("person_name") and counts.get("legal_case_context"):
            score = max(score, 7)

        llm_result = self._maybe_review_with_llm(sample, counts, score, entities)
        if llm_result is not None and llm_result.get("confidence", 0.0) >= 0.75:
            if llm_result.get("is_hit"):
                return self.make_result(
                    sample,
                    True,
                    min(0.95, max(0.75, llm_result.get("confidence", 0.0))),
                    self._locations_from_llm(llm_result),
                    rules + ["llm-text-id-review"],
                    f"LLM review: {llm_result.get('reason')}",
                    ["desensitize"],
                )
            return self.make_result(
                sample,
                False,
                1.0 - min(0.95, llm_result.get("confidence", 0.0)),
                [],
                rules + ["llm-text-id-review"],
                f"LLM review rejected text_id: {llm_result.get('reason')}",
                ["allow"],
            )

        if score < 7:
            return self.no_hit(sample, f"Entity combination score {score} is below text_id threshold.")

        confidence = 0.86 if score < 10 else 0.92
        relation_rule_ids = sorted({item["rule_id"] for item in relationships})
        if relation_rule_ids:
            reason = "Identifying entity relationship(s) found: " + ", ".join(relation_rule_ids) + "."
        else:
            reason = f"Free-text entity combination score reached {score}, indicating identifiable person/object risk."
        return self.make_result(
            sample,
            True,
            confidence,
            self._dedupe_locations(locations),
            rules,
            reason,
            ["desensitize"],
        )

    def _extract_entities(self, text: str) -> List[Dict[str, Any]]:
        entities: List[Dict[str, Any]] = []

        def add_entity(entity_type: str, value: str, start: int, end: int, quality: str = "full") -> None:
            value = value.strip(" \t\r\n，。；;：:、（）()“”\"「」")
            if not value:
                return
            adjusted_start = text.find(value, start, end)
            if adjusted_start >= 0:
                start = adjusted_start
                end = start + len(value)
            key = (entity_type, start, end, value)
            if any((item["type"], item["start"], item["end"], item["text"]) == key for item in entities):
                return
            entities.append(
                {
                    "type": entity_type,
                    "text": value,
                    "start": start,
                    "end": end,
                    "quality": quality,
                }
            )

        for match in self.PERSON_LABEL.finditer(text):
            value = match.group(1)
            start, end = match.span(1)
            quality = "masked" if "某" in value or "*" in value else "full"
            add_entity("person", value, start, end, quality)

        for match in self.PERSON_CANDIDATE.finditer(text):
            value = match.group(1)
            if not self._person_candidate_has_context(text, match.start(1), match.end(1)):
                continue
            quality = (
                "title"
                if re.search(r"(?:主任|老师|先生|女士|经理|局长|队长)$", value)
                else "masked"
                if "某" in value or "*" in value
                else "full"
            )
            add_entity("person", value, match.start(1), match.end(1), quality)

        for pattern in (self.ALIAS, self.QUOTED_ALIAS):
            for match in pattern.finditer(text):
                add_entity("person", match.group(1), match.start(1), match.end(1), "alias")

        for pattern, entity_type in ((self.MASKED_ID, "id_number"), (self.MASKED_PHONE, "phone")):
            for match in pattern.finditer(text):
                add_entity(entity_type, match.group(0), match.start(), match.end())

        for match in self.ADDRESS_SHAPE.finditer(text):
            add_entity("address", match.group(0), match.start(), match.end())
        for match in self.ADDRESS_NUMBERED.finditer(text):
            add_entity("address", match.group(0), match.start(), match.end())
        for match in self.ADDRESS_LABEL.finditer(text):
            value = match.group(1)
            if re.search(r"(?:\d|XX).{0,8}(?:号院|号|栋|幢|座|楼|单元|门|室|层|平房)", value):
                add_entity("address", value, match.start(1), match.end(1))

        for match in self.ORGANIZATION.finditer(text):
            add_entity("organization", match.group(0), match.start(), match.end())
        for match in self.CASE_ID.finditer(text):
            add_entity("case_id", match.group(0), match.start(), match.end())

        for entity_type, words in (
            ("role", self.ROLE_WORDS),
            ("position", self.POSITION_WORDS),
            ("event", self.EVENT_WORDS),
        ):
            for word in sorted(words, key=len, reverse=True):
                for match in re.finditer(re.escape(word), text):
                    add_entity(entity_type, word, match.start(), match.end())

        entities.sort(key=lambda item: (item["start"], item["end"], item["type"]))
        return entities

    def _person_candidate_has_context(self, text: str, start: int, end: int) -> bool:
        suffix = text[end : end + 4]
        if suffix.startswith(("在", "为", "是", "作为", "现任", "担任", "负责", "承办", "参与", "跟进", "完成", "处理")):
            return True
        if start == 0 or text[start - 1] in " \t\r\n，。；;：:“\"「（(":
            return True
        prefix = text[max(0, start - 32) : start]
        if any(prefix.endswith(word) for word in self.POSITION_WORDS):
            return True
        if prefix.endswith(("将", "把", "列出", "列明", "写有", "包含")):
            return True
        if prefix.endswith(("是", "为")):
            stem = prefix[:-1]
            if any(stem.endswith(word) for word in self.POSITION_WORDS + self.ROLE_WORDS):
                return True
        return bool(
            re.search(
                r"(?:请将|公开|写明|显示|材料|记录|报告|附件|回访单|日志注释|答复草稿|输出报告|"
                r"执法队|支队|管理局|居委会|街道办|民政办|服务站|监管所|资源所|中心|社区|物业|公司|集团|医院|"
                r"办公室|服务科|窗口|专班|中队|科)$",
                prefix,
            )
        )

    def _find_relationships(self, text: str, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Explicitly anonymized examples and policy explanations must not be
        # promoted merely because their prose names entity categories.
        if self.NEGATIVE_CONTEXT.search(text):
            return []

        relationships: List[Dict[str, Any]] = []

        def add_relation(
            rule_id: str,
            entity_types: Sequence[str],
            max_span: int = 180,
            require_full_person: bool = True,
        ) -> None:
            candidates = [[item for item in entities if item["type"] == entity_type] for entity_type in entity_types]
            if any(not items for items in candidates):
                return
            for combination in product(*candidates):
                if len({(item["type"], item["start"], item["end"]) for item in combination}) != len(combination):
                    continue
                person = next((item for item in combination if item["type"] == "person"), None)
                if require_full_person and person is not None and person.get("quality") != "full":
                    continue
                if not self._entities_share_context(text, combination, max_span):
                    continue
                relationships.append({"rule_id": rule_id, "entities": list(combination)})
                return

        add_relation("person-id-relation", ("person", "id_number"))
        add_relation("person-phone-relation", ("person", "phone"))
        add_relation("person-address-relation", ("person", "address"), max_span=150)
        add_relation(
            "person-organization-position-relation",
            ("person", "organization", "position"),
            max_span=180,
        )
        add_relation("person-role-event-relation", ("person", "role", "event"), max_span=180)
        add_relation("person-organization-event-relation", ("person", "organization", "event"), max_span=180)
        add_relation("person-position-event-relation", ("person", "position", "event"), max_span=180)
        add_relation("person-case-event-relation", ("person", "case_id", "event"), max_span=220)

        # A masked name or nickname can still identify a person when the text
        # states uniqueness or binds it to a precise scope, position and event.
        if re.search(r"唯一|全部|可定位|识别风险|负责范围|常驻", text):
            add_relation(
                "quasi-person-position-event-relation",
                ("person", "position", "event"),
                max_span=220,
                require_full_person=False,
            )
            add_relation(
                "quasi-person-organization-position-relation",
                ("person", "organization", "position"),
                max_span=220,
                require_full_person=False,
            )

        unique = []
        seen = set()
        for relationship in relationships:
            if relationship["rule_id"] in seen:
                continue
            seen.add(relationship["rule_id"])
            unique.append(relationship)
        return unique

    def _entities_share_context(
        self,
        text: str,
        entities: Sequence[Dict[str, Any]],
        max_span: int,
    ) -> bool:
        start = min(item["start"] for item in entities)
        end = max(item["end"] for item in entities)
        if end - start > max_span:
            return False
        between = text[start:end]
        return not re.search(r"[。！？!?；;\n]", between)

    def _maybe_review_with_llm(
        self,
        sample: Dict[str, Any],
        counts: Dict[str, int],
        score: int,
        entities: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if not self.use_llm or self.llm_adapter is None or not self.llm_adapter.is_available():
            return None
        is_boundary = sample.get("sample_kind") == "boundary"
        has_full_person = any(
            entity.get("type") == "person" and entity.get("quality") == "full"
            for entity in entities
        )
        if not is_boundary and not (4 <= score <= 8) and not has_full_person:
            return None
        signals = {
            "score": score,
            "counts": counts,
            "sample_kind": sample.get("sample_kind"),
            "has_full_person_candidate": has_full_person,
            "entity_candidates": [
                {
                    "type": entity.get("type"),
                    "text": entity.get("text"),
                    "quality": entity.get("quality"),
                }
                for entity in entities[:20]
            ],
        }
        return self.llm_adapter.review_text_id(sample, signals, score)

    def _locations_from_llm(self, llm_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        locations = []
        for span in llm_result.get("risk_spans", []):
            if not isinstance(span, dict):
                continue
            text = str(span.get("text") or "").strip()
            if not text:
                continue
            locations.append(
                self.make_location(
                    "content_text",
                    str(llm_result.get("risk_type") or "llm_text_id"),
                    text[:160],
                    "llm-text-id-review",
                )
            )
        if not locations and llm_result.get("is_hit"):
            locations.append(
                self.make_location(
                    "content_text",
                    str(llm_result.get("risk_type") or "llm_text_id"),
                    str(llm_result.get("reason") or "LLM identified text_id risk")[:160],
                    "llm-text-id-review",
                )
            )
        return locations

    def _dedupe_locations(self, locations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        unique = []
        for location in locations:
            key = (location.get("risk_type"), location.get("text"))
            if key in seen:
                continue
            seen.add(key)
            unique.append(location)
        return unique

