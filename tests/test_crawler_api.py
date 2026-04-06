from __future__ import annotations

from crawler.flk_api import FlkApi, _normalize_status, parse_source_id_from_url
from crawler.models import LawListItem


class DummyClient:
    base_url = "https://flk.npc.gov.cn"


def test_metadata_from_list_item_keeps_top_level_category() -> None:
    api = FlkApi(DummyClient())
    item = LawListItem(
        source_id="402881e45ffbbe41015ffc66d4160c13",
        title="人民检察院刑事诉讼规则（试行）",
        category="司法解释",
        issuer="最高人民检察院",
        publish_date="2012-11-22",
        effective_date="2013-01-01",
        status="已修改",
    )

    metadata = api.metadata_from_list_item(item)

    assert metadata.category == "司法解释"
    assert metadata.source_url == (
        "https://flk.npc.gov.cn/detail"
        "?id=402881e45ffbbe41015ffc66d4160c13"
        "&title=%E4%BA%BA%E6%B0%91%E6%A3%80%E5%AF%9F%E9%99%A2%E5%88%91%E4%BA%8B%E8%AF%89%E8%AE%BC"
        "%E8%A7%84%E5%88%99%EF%BC%88%E8%AF%95%E8%A1%8C%EF%BC%89"
    )


def test_parse_source_id_from_new_detail_url() -> None:
    source_id = parse_source_id_from_url(
        "https://flk.npc.gov.cn/detail"
        "?id=402881e45ffbbe41015ffc66d4160c13"
        "&title=%E4%BA%BA%E6%B0%91%E6%A3%80%E5%AF%9F%E9%99%A2%E5%88%91%E4%BA%8B%E8%AF%89%E8%AE%BC"
        "%E8%A7%84%E5%88%99%EF%BC%88%E8%AF%95%E8%A1%8C%EF%BC%89"
    )

    assert source_id == "402881e45ffbbe41015ffc66d4160c13"


def test_status_mapping_matches_verified_flk_samples() -> None:
    assert _normalize_status(1) == "已废止"
    assert _normalize_status(2) == "已修改"
    assert _normalize_status(3) == "现行有效"
    assert _normalize_status(4) == "尚未生效"
