"""
Improved Detail page parser for NuriJangter bid notices.

This version uses more robust strategies for extracting data from WebSquare-based pages.
"""

from typing import Dict, List, Any, Optional
from playwright.sync_api import Page, Frame
import logging
import re

from ..models.schema import AttachedFile

logger = logging.getLogger(__name__)


class DetailPageParserImproved:
    """
    Improved parser with multiple extraction strategies for NuriJangter detail pages.

    Strategies:
    1. XPath-based TH-TD following-sibling
    2. Table row scanning with flexible matching
    3. Label-value pair extraction with fuzzy matching
    4. Frame-aware parsing
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize improved detail page parser."""
        self.config = config
        self.detail_fields = config.get('extraction', {}).get('detail_fields', [])

    def parse_page(self, page: Page, base_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a detail page with improved extraction logic.

        Args:
            page: Playwright page object (could be new tab from list page)
            base_data: Base data from list page

        Returns:
            Complete bid notice data dictionary
        """
        notice = base_data.copy()

        try:
            # Wait for content
            try:
                page.wait_for_selector('table, .w2window_content_body', timeout=10000)
            except:
                logger.warning("Timeout waiting for detail page content")

            # Find the best frame to parse (WebSquare often uses iframes)
            target_frame = self._find_best_frame(page)

            logger.info(f"Parsing detail page, using frame: {target_frame.name if hasattr(target_frame, 'name') else 'main'}")

            # Extract all table data first
            all_data = self._extract_all_table_data(target_frame)

            logger.debug(f"Extracted {len(all_data)} key-value pairs from tables")

            # Map extracted data to our schema
            notice.update(self._map_to_schema(all_data))

            # Extract attached files
            attached_files = self._parse_attached_files(target_frame)
            if attached_files:
                notice['attached_files'] = attached_files

            # Store source URL
            notice['source_url'] = page.url

        except Exception as e:
            logger.error(f"Failed to parse detail page: {e}", exc_info=True)

        return notice

    def _find_best_frame(self, page: Page):
        """
        Find the frame with the most content (likely the main content frame).

        Returns:
            Page or Frame object
        """
        frames = page.frames

        if len(frames) == 1:
            return page

        # Score each frame by table count
        best_frame = page
        max_score = len(page.query_selector_all('table'))

        for frame in frames:
            try:
                table_count = len(frame.query_selector_all('table'))
                if table_count > max_score:
                    max_score = table_count
                    best_frame = frame
                    logger.debug(f"Selected frame '{frame.name or 'unnamed'}' with {table_count} tables")
            except:
                continue

        return best_frame

    def _extract_all_table_data(self, frame) -> Dict[str, str]:
        """
        Extract all key-value pairs from all tables using multiple strategies.

        Returns:
            Dictionary of label -> value pairs
        """
        data = {}

        # Strategy 1: XPath - TH with following TD
        try:
            # Find all TH elements
            ths = frame.query_selector_all('th')
            for th in ths:
                try:
                    label = self._clean_text(th.inner_text())
                    if not label:
                        continue

                    # Try to find following TD using JS
                    td_text = th.evaluate("""
                        (el) => {
                            const td = el.nextElementSibling;
                            if (td && td.tagName === 'TD') {
                                return td.innerText;
                            }
                            return null;
                        }
                    """)

                    if td_text:
                        value = self._clean_text(td_text)
                        if value:
                            data[label] = value
                            logger.debug(f"[XPath] {label} = {value[:50]}")
                except Exception as e:
                    logger.debug(f"Failed to extract from TH: {e}")
                    continue
        except Exception as e:
            logger.debug(f"Strategy 1 failed: {e}")

        # Strategy 2: Table row iteration
        try:
            tables = frame.query_selector_all('table')
            for table in tables:
                rows = table.query_selector_all('tr')
                for row in rows:
                    try:
                        # Get all cells
                        cells = row.query_selector_all('th, td')

                        # Pattern 1: [TH] [TD] [TH] [TD] ...
                        if len(cells) >= 2:
                            i = 0
                            while i < len(cells) - 1:
                                cell1 = cells[i]
                                cell2 = cells[i + 1]

                                tag1 = cell1.evaluate("el => el.tagName")
                                tag2 = cell2.evaluate("el => el.tagName")

                                # TH followed by TD
                                if tag1 == "TH" and tag2 == "TD":
                                    label = self._clean_text(cell1.inner_text())
                                    value = self._clean_text(cell2.inner_text())

                                    if label and value:
                                        if label not in data:  # Don't overwrite
                                            data[label] = value
                                            logger.debug(f"[Table] {label} = {value[:50]}")

                                    i += 2
                                else:
                                    i += 1
                    except Exception as e:
                        logger.debug(f"Failed to parse row: {e}")
                        continue
        except Exception as e:
            logger.debug(f"Strategy 2 failed: {e}")

        # Strategy 3: Label-value divs/spans (less common in tables but worth trying)
        try:
            # Look for .label or .th class followed by .value or .td class
            label_elements = frame.query_selector_all('.label, .th, span.label, div.label')
            for label_el in label_elements:
                try:
                    label = self._clean_text(label_el.inner_text())
                    if not label:
                        continue

                    # Try to find value element
                    value_el = label_el.evaluate_handle("""
                        (el) => {
                            // Try next sibling
                            let sibling = el.nextElementSibling;
                            if (sibling && (sibling.classList.contains('value') ||
                                           sibling.classList.contains('td') ||
                                           sibling.tagName === 'TD')) {
                                return sibling;
                            }

                            // Try parent's next child
                            let parent = el.parentElement;
                            if (parent) {
                                let children = Array.from(parent.children);
                                let idx = children.indexOf(el);
                                if (idx >= 0 && idx < children.length - 1) {
                                    return children[idx + 1];
                                }
                            }

                            return null;
                        }
                    """)

                    if value_el:
                        value = self._clean_text(value_el.evaluate("el => el.innerText"))
                        if value and label not in data:
                            data[label] = value
                            logger.debug(f"[Div] {label} = {value[:50]}")
                except:
                    continue
        except Exception as e:
            logger.debug(f"Strategy 3 failed: {e}")

        return data

    def _map_to_schema(self, raw_data: Dict[str, str]) -> Dict[str, Any]:
        """
        Map extracted raw data to our BidNotice schema fields.

        Uses fuzzy matching to handle variations in Korean labels.
        """
        mapped = {}

        # Field mapping: schema_field -> possible Korean labels
        field_mappings = {
            'classification': ['분류', '물품분류', '용역분류', '공고분류', '입찰분류'],
            'demanding_agency': ['수요기관', '발주기관'],
            'bid_method': ['입찰방법', '계약방법', '낙찰방법'],
            'budget_amount': ['예산금액', '추정금액', '추정가격'],
            'estimated_price': ['기초금액', '추정가격', '예정가격'],
            'base_price': ['예정가격', '기준가격'],
            'pre_qualification': ['사전규격', '사전심사', '적격심사'],
            'qualification_requirements': ['자격요건', '입찰참가자격', '참가자격'],
            'guarantee_rate': ['보증금율', '계약보증금율'],
            'bid_bond': ['입찰보증금', '입찰보증'],
            'contract_bond': ['계약보증금', '계약이행보증', '이행보증금'],
            'payment_terms': ['대금지급방법', '계약금지급', '대금지급조건'],
            'delivery_location': ['납품장소', '계약장소', '이행장소', '장소'],
            'delivery_deadline': ['납품기한', '이행기간', '납품기간'],
            'contract_period': ['계약기간', '이행기간'],
            'contact_person': ['담당자', '계약담당자', '담당자명'],
            'contact_department': ['부서', '담당부서', '소속'],
            'phone_number': ['전화번호', '연락처', '담당자 전화번호'],
            'fax_number': ['팩스', 'FAX', '팩스번호'],
            'email': ['이메일', 'E-mail', '전자우편'],
            'bid_date': ['입찰일시', '입찰개시일시'],
            'opening_date': ['개찰일시', '개찰일'],
            'specifications': ['세부규격', '과업내용', '규격', '내역'],
            'evaluation_criteria': ['평가기준', '낙찰기준', '낙찰자결정방법'],
            'eligible_entities': ['참가자격', '입찰참가자격제한', '적격업체'],
            'notes': ['비고', '특이사항', '참고사항'],
            'status': ['상태', '진행상태', '진행현황']
        }

        # Fuzzy match each field
        for schema_field, possible_labels in field_mappings.items():
            value = self._find_value_by_labels(raw_data, possible_labels)
            if value:
                mapped[schema_field] = value

        return mapped

    def _find_value_by_labels(self, data: Dict[str, str], labels: List[str]) -> Optional[str]:
        """
        Find a value in data dict by trying multiple label variations.

        Uses fuzzy matching to handle:
        - Extra spaces
        - Trailing colons
        - Parentheses with units
        """
        for label in labels:
            # Exact match
            if label in data:
                return data[label]

            # Fuzzy match - check if label is substring
            for key, value in data.items():
                # Remove common suffixes and normalize
                normalized_key = key.strip().rstrip(':').strip()
                normalized_label = label.strip().rstrip(':').strip()

                if normalized_label in normalized_key or normalized_key in normalized_label:
                    return value

        return None

    def _parse_attached_files(self, frame) -> List[AttachedFile]:
        """
        Extract attached files from detail page.

        NuriJangter typically uses grid with id containing 'grdFile' or similar.
        """
        files = []

        try:
            # Strategy 1: Grid with 'file' in ID
            file_divs = frame.query_selector_all('div[id*="grdFile"], div[id*="File"], div[id*="file"]')

            for div in file_divs:
                try:
                    file_links = div.query_selector_all('a')

                    for link in file_links:
                        try:
                            filename = self._clean_text(link.inner_text())
                            if not filename or len(filename) < 2:
                                continue

                            url = link.get_attribute('href') or ""

                            # Try to find file size in same row
                            size = None
                            try:
                                row = link.evaluate_handle("el => el.closest('tr')")
                                if row:
                                    row_text = row.evaluate("el => el.innerText")
                                    # Look for patterns like "123KB", "1.5MB"
                                    size_match = re.search(r'(\d+\.?\d*\s*[KMG]?B)', row_text, re.IGNORECASE)
                                    if size_match:
                                        size = size_match.group(1)
                            except:
                                pass

                            # Determine file type from extension
                            file_type = None
                            if '.' in filename:
                                file_type = filename.split('.')[-1].lower()

                            files.append(AttachedFile(
                                filename=filename,
                                url=url,
                                size=size,
                                file_type=file_type
                            ))

                            logger.debug(f"Found file: {filename} ({size})")
                        except Exception as e:
                            logger.debug(f"Failed to parse file link: {e}")
                            continue
                except Exception as e:
                    logger.debug(f"Failed to parse file div: {e}")
                    continue

            # Strategy 2: Look for common file attachment areas
            if not files:
                file_sections = frame.query_selector_all('.file_list, .attach_list, .attachment')
                for section in file_sections:
                    links = section.query_selector_all('a')
                    for link in links:
                        try:
                            filename = self._clean_text(link.inner_text())
                            if filename and len(filename) > 2:
                                files.append(AttachedFile(
                                    filename=filename,
                                    url=link.get_attribute('href') or ""
                                ))
                        except:
                            continue

        except Exception as e:
            logger.debug(f"Error parsing attached files: {e}")

        logger.info(f"Found {len(files)} attached files")
        return files

    def _clean_text(self, text: str) -> str:
        """
        Clean extracted text.

        Handles:
        - Extra whitespace
        - Newlines
        - Common label suffixes
        """
        if not text:
            return ""

        # Replace multiple whitespace with single space
        text = re.sub(r'\s+', ' ', text)

        # Strip
        text = text.strip()

        # Remove trailing colons and spaces
        text = re.sub(r'[:\s]+$', '', text)
        text = re.sub(r'^[:\s]+', '', text)

        return text
