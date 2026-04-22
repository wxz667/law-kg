"""刑事一审案件XML文书解析器 - 用于学习法条引用格式"""
import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional
from pathlib import Path
import re


class CriminalCaseXMLParser:
    """解析刑事一审案件XML文书，提取法条引用格式"""

    def __init__(self):
        self.namespace = {}

    def parse_file(self, file_path: str | Path) -> Dict[str, Any]:
        """解析XML文件"""
        tree = ET.parse(file_path)
        root = tree.getroot()
        return self._parse_document(root)

    def parse_string(self, xml_string: str) -> Dict[str, Any]:
        """解析XML字符串"""
        root = ET.fromstring(xml_string)
        return self._parse_document(root)

    def _parse_document(self, root: ET.Element) -> Dict[str, Any]:
        """解析完整文档"""
        qw = root.find('QW')
        if qw is None:
            raise ValueError("Invalid XML: missing QW element")

        doc = {
            'title': self._get_attr(qw, 'title'),
            'case_number': self._extract_case_number(qw),
            'court': self._extract_court(qw),
            'defendants': self._extract_defendants(qw),
            'charges': self._extract_charges(qw),
            'law_citations': self._extract_law_citations(qw),
            'verdict': self._extract_verdict(qw),
            'date': self._extract_date(qw),
        }

        return doc

    def extract_law_citation_format(self, xml_content: str) -> List[Dict[str, str]]:
        """
        从XML文书中提取法条引用格式
        返回格式化的法条引用列表，参考刑事判决书标准格式
        """
        try:
            root = ET.fromstring(xml_content)
            qw = root.find('QW')
            if qw is None:
                return []

            # 从裁判分析过程中提取法条引用
            cpfxgc = qw.find('CPFXGC')
            citations = []

            if cpfxgc is not None:
                flftyy = cpfxgc.find('FLFTYY')
                if flftyy is not None:
                    for flftfz in flftyy.findall('FLFTFZ'):
                        law_name = self._get_text(flftfz, 'MC')
                        if law_name:
                            for tiao in flftfz.findall('T'):
                                tiao_num = tiao.get(
                                    'nameCN') or tiao.get('value', '')
                                kuan = self._get_text(tiao, 'K')
                                xiang = self._get_text(tiao, 'X')

                                citation = {
                                    'law_name': law_name,
                                    'article': tiao_num,
                                    'paragraph': kuan or '',
                                    'item': xiang or '',
                                }
                                citations.append(citation)

            return citations
        except Exception as e:
            print(f"解析法条引用失败: {e}")
            return []

    def format_law_citation(self, law_name: str, article: str,
                            paragraph: str = '', item: str = '') -> str:
        """
        格式化为刑事判决书标准法条引用格式
        例如：《中华人民共和国刑法》第二百二十五条第（一）项
        """
        if not article:
            return law_name

        result = f"《{law_name}》{article}"
        if paragraph:
            result += paragraph
        if item:
            result += item

        return result

    def format_citations_for_appendix(self, citations: List[Dict[str, str]]) -> str:
        """
        格式化法条引用为文末附录格式
        例如：依照《中华人民共和国刑法》第二百二十五条第（一）项、第一百四十条...之规定
        """
        if not citations:
            return ""

        # 按法律名称分组
        law_groups = {}
        for cit in citations:
            law_name = cit['law_name']
            if law_name not in law_groups:
                law_groups[law_name] = []

            article_str = cit['article']
            if cit['paragraph']:
                article_str += cit['paragraph']
            if cit['item']:
                article_str += cit['item']

            law_groups[law_name].append(article_str)

        # 格式化输出
        parts = []
        for law_name, articles in law_groups.items():
            if len(articles) == 1:
                parts.append(f"《{law_name}》{articles[0]}")
            else:
                articles_str = '、'.join(articles)
                parts.append(f"《{law_name}》{articles_str}")

        if parts:
            citations_str = '、'.join(parts)
            return f"\n\n附录：相关法律条文\n{citations_str}"

        return ""

    def _get_attr(self, element: ET.Element, attr_name: str) -> str:
        """获取元素属性"""
        return element.get(attr_name) or ''

    def _get_text(self, element: ET.Element, tag_name: str) -> str:
        """获取子元素文本"""
        child = element.find(tag_name)
        if child is not None:
            return child.text or ''
        return ''

    def _extract_case_number(self, qw: ET.Element) -> str:
        """提取案号"""
        ws = qw.find('WS')
        if ws is not None:
            ah = ws.find('AH')
            if ah is not None:
                return ah.get('value') or ''
        return ''

    def _extract_court(self, qw: ET.Element) -> str:
        """提取法院名称"""
        ws = qw.find('WS')
        if ws is not None:
            jbfy = ws.find('JBFY')
            if jbfy is not None:
                return jbfy.get('value') or ''
        return ''

    def _extract_defendants(self, qw: ET.Element) -> List[str]:
        """提取被告人列表"""
        dsr = qw.find('DSR')
        defendants = []
        if dsr is not None:
            for ysf in dsr.findall('YSF'):
                sscyr = ysf.find('SSCYR')
                if sscyr is not None:
                    name = sscyr.get('value') or sscyr.text or ''
                    if name:
                        defendants.append(name)
        return defendants

    def _extract_charges(self, qw: ET.Element) -> List[str]:
        """提取罪名"""
        ssjl = qw.find('SSJL')
        charges = []
        if ssjl is not None:
            zxxx = ssjl.find('ZKXX')
            if zxxx is not None:
                for zksj in zxxx.findall('ZKJL'):
                    zkzm = zksj.find('ZKZM')
                    if zkzm is not None:
                        charge = zkzm.get('value') or ''
                        if charge and charge not in charges:
                            charges.append(charge)
        return charges

    def _extract_law_citations(self, qw: ET.Element) -> List[Dict[str, str]]:
        """提取法条引用"""
        return self.extract_law_citation_format(ET.tostring(qw, encoding='unicode'))

    def _extract_verdict(self, qw: ET.Element) -> str:
        """提取判决结果"""
        pjjg = qw.find('PJJG')
        if pjjg is not None:
            return pjjg.get('value') or ''
        return ''

    def _extract_date(self, qw: ET.Element) -> str:
        """提取裁判日期"""
        ww = qw.find('WW')
        if ww is not None:
            cpsj = ww.find('CPSJ')
            if cpsj is not None:
                return cpsj.get('value') or ''
        return ''


def parse_xml_file(file_path: str | Path) -> Dict[str, Any]:
    """便捷函数：解析XML文件"""
    parser = CriminalCaseXMLParser()
    return parser.parse_file(file_path)


def extract_citations_from_xml(xml_content: str) -> List[Dict[str, str]]:
    """便捷函数：从XML内容提取法条引用"""
    parser = CriminalCaseXMLParser()
    return parser.extract_law_citation_format(xml_content)


def format_citation_for_appendix(citations: List[Dict[str, str]]) -> str:
    """便捷函数：格式化为附录引用"""
    parser = CriminalCaseXMLParser()
    return parser.format_citations_for_appendix(citations)
