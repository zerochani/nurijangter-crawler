"""
Detail page parser for NuriJangter bid notices.

This module extracts detailed information from individual bid notice pages.
"""

from typing import Dict, List, Any, Optional
from playwright.sync_api import Page
import logging
import re

from ..models.schema import AttachedFile

logger = logging.getLogger(__name__)


class DetailPageParser:
    """
    Parser for extracting detailed information from bid notice detail pages.

    Extracts comprehensive information including specifications, requirements,
    contact information, and attached files.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize detail page parser.

        Args:
            config: Configuration dictionary with extraction settings
        """
        self.config = config
        self.detail_fields = config.get('extraction', {}).get('detail_fields', [])

    def parse_page(self, page: Page, base_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a detail page and extract all information.

        Args:
            page: Playwright page object
            base_data: Base data from list page

        Returns:
            Complete bid notice data dictionary
        """
        notice = base_data.copy()

        try:
            # Wait for main content to load - Broaden selector for WebSquare
            # WebSquare often uses w2window_content_body or just loads content
            try:
                page.wait_for_selector('.detail_box, .view_box, #container, .w2window_content_body, table', timeout=10000)
            except:
                logger.warning("Main detail container not found, attempting parse anyway")

            # Support iframe content (WebSquare often loads detail in iframe)
            target_frame = page
            # If main page has few tables but there's an iframe, switch to it
            if len(page.query_selector_all('table')) < 2 and len(page.frames) > 1:
                for frame in page.frames:
                    if len(frame.query_selector_all('table')) > 2:
                        target_frame = frame
                        logger.info(f"Switched parsing to frame: {frame.name or 'unknown'}")
                        break

            # Extract various sections using the best frame
            notice.update(self._parse_basic_info(target_frame))
            notice.update(self._parse_financial_info(target_frame))
            notice.update(self._parse_dates(target_frame))
            notice.update(self._parse_requirements(target_frame))
            notice.update(self._parse_contract_info(target_frame))
            notice.update(self._parse_contact_info(target_frame))
            notice.update(self._parse_specifications(target_frame))

            # Extract attached files
            attached_files = self._parse_attached_files(page)
            if attached_files:
                notice['attached_files'] = attached_files

            # Store source URL
            notice['source_url'] = page.url

        except Exception as e:
            logger.error(f"Failed to parse detail page: {e}")

        return notice

    def _parse_basic_info(self, page: Page) -> Dict[str, Any]:
        """Extract basic information section."""
        info = {}

        try:
            # Classification/category
            classification = self._find_table_value(page, ['분류', '물품분류', '용역분류'])
            if classification:
                info['classification'] = classification

            # Demanding agency
            demanding_agency = self._find_table_value(page, ['수요기관', '발주기관'])
            if demanding_agency:
                info['demanding_agency'] = demanding_agency

            # Eligible entities
            eligible = self._find_table_value(page, ['참가자격', '입찰참가자격'])
            if eligible:
                info['eligible_entities'] = eligible

            # Status
            status = self._find_table_value(page, ['상태', '진행상태'])
            if status:
                info['status'] = status

        except Exception as e:
            logger.debug(f"Error parsing basic info: {e}")

        return info

    def _parse_financial_info(self, page: Page) -> Dict[str, Any]:
        """Extract financial information."""
        info = {}

        try:
            # Budget amount
            budget = self._find_table_value(page, ['예산금액', '추정가격'])
            if budget:
                info['budget_amount'] = budget

            # Estimated price
            estimated = self._find_table_value(page, ['기초금액', '추정가격'])
            if estimated:
                info['estimated_price'] = estimated

            # Base price
            base_price = self._find_table_value(page, ['예정가격', '기준가격'])
            if base_price:
                info['base_price'] = base_price

            # Guarantee rate
            guarantee_rate = self._find_table_value(page, ['보증금율', '계약보증금율'])
            if guarantee_rate:
                info['guarantee_rate'] = guarantee_rate

            # Bid bond
            bid_bond = self._find_table_value(page, ['입찰보증금', '입찰보증'])
            if bid_bond:
                info['bid_bond'] = bid_bond

            # Contract bond
            contract_bond = self._find_table_value(page, ['계약보증금', '계약이행보증'])
            if contract_bond:
                info['contract_bond'] = contract_bond

        except Exception as e:
            logger.debug(f"Error parsing financial info: {e}")

        return info

    def _parse_dates(self, page: Page) -> Dict[str, Any]:
        """Extract date information."""
        info = {}

        try:
            # Bid date
            bid_date = self._find_table_value(page, ['입찰일시', '개찰일시'])
            if bid_date:
                info['bid_date'] = bid_date

            # Opening date
            opening_date = self._find_table_value(page, ['개찰일시'])
            if opening_date:
                info['opening_date'] = opening_date

        except Exception as e:
            logger.debug(f"Error parsing dates: {e}")

        return info

    def _parse_requirements(self, page: Page) -> Dict[str, Any]:
        """Extract requirement information."""
        info = {}

        try:
            # Pre-qualification
            pre_qual = self._find_table_value(page, ['사전규격', '사전심사'])
            if pre_qual:
                info['pre_qualification'] = pre_qual

            # Qualification requirements
            qual_req = self._find_table_value(page, ['자격요건', '입찰참가자격'])
            if qual_req:
                info['qualification_requirements'] = qual_req

            # Evaluation criteria
            eval_criteria = self._find_table_value(page, ['평가기준', '낙찰기준', '낙찰자결정방법'])
            if eval_criteria:
                info['evaluation_criteria'] = eval_criteria

        except Exception as e:
            logger.debug(f"Error parsing requirements: {e}")

        return info

    def _parse_contract_info(self, page: Page) -> Dict[str, Any]:
        """Extract contract-related information."""
        info = {}

        try:
            # Payment terms
            payment = self._find_table_value(page, ['대금지급방법', '계약금지급'])
            if payment:
                info['payment_terms'] = payment

            # Delivery location
            location = self._find_table_value(page, ['납품장소', '계약장소', '이행장소'])
            if location:
                info['delivery_location'] = location

            # Delivery deadline
            deadline = self._find_table_value(page, ['납품기한', '이행기간', '계약기간'])
            if deadline:
                info['delivery_deadline'] = deadline

            # Contract period
            period = self._find_table_value(page, ['계약기간'])
            if period:
                info['contract_period'] = period

        except Exception as e:
            logger.debug(f"Error parsing contract info: {e}")

        return info

    def _parse_contact_info(self, page: Page) -> Dict[str, Any]:
        """Extract contact information."""
        info = {}

        try:
            # Contact person
            contact = self._find_table_value(page, ['담당자', '계약담당자'])
            if contact:
                info['contact_person'] = contact

            # Department
            dept = self._find_table_value(page, ['부서', '담당부서'])
            if dept:
                info['contact_department'] = dept

            # Phone number
            phone = self._find_table_value(page, ['전화번호', '연락처'])
            if phone:
                info['phone_number'] = phone

            # Fax
            fax = self._find_table_value(page, ['팩스', 'FAX'])
            if fax:
                info['fax_number'] = fax

            # Email
            email = self._find_table_value(page, ['이메일', 'E-mail'])
            if email:
                info['email'] = email

        except Exception as e:
            logger.debug(f"Error parsing contact info: {e}")

        return info

    def _parse_specifications(self, page: Page) -> Dict[str, Any]:
        """Extract specifications and detailed content."""
        info = {}

        try:
            # Specifications/scope of work
            spec_selectors = [
                '.spec_box',
                '.content_box',
                'div:has-text("세부규격")',
                'div:has-text("과업내용")'
            ]

            for selector in spec_selectors:
                element = page.locator(selector).first
                if element.count() > 0:
                    text = self._clean_text(element.inner_text())
                    if text and len(text) > 10:
                        info['specifications'] = text
                        break

            # Additional notes
            notes_selectors = [
                '.note_box',
                'div:has-text("비고")',
                'div:has-text("특이사항")'
            ]

            for selector in notes_selectors:
                element = page.locator(selector).first
                if element.count() > 0:
                    text = self._clean_text(element.inner_text())
                    if text and len(text) > 5:
                        info['notes'] = text
                        break

        except Exception as e:
            logger.debug(f"Error parsing specifications: {e}")

        return info

    def _parse_attached_files(self, page: Page) -> List[AttachedFile]:
        """Extract attached files."""
        files = []

        try:
            # 1. NuriJangter Grid File Attachments
            # Selector: //div[contains(@id, "grdFile")]//a
            file_links = page.locator('//div[contains(@id, "grdFile")]//a, .file_list a, .attach_list a').all()

            for link in file_links:
                try:
                    filename = self._clean_text(link.inner_text())
                    if not filename:
                        continue
                        
                    url = link.get_attribute('href')
                    # If URL is javascript, might need handling, but usually for downloads it's fine or handled by browser download behavior.
                    # For scraping metadata, we just store what we find.

                    # Extract file size if available (often in separate column or text)
                    # In grid, it might be in a sibling cell
                    size = None
                    try:
                        # Try to find size in the row if it's a grid
                        row = link.locator('xpath=./ancestor::tr').first
                        if row.count() > 0:
                            # Assuming size is in a column with "KB" or "MB"
                            size_cell = row.locator('td:has-text("KB"), td:has-text("MB"), td:has-text("B")').first
                            if size_cell.count() > 0:
                                size = self._clean_text(size_cell.inner_text())
                    except:
                        pass
                    
                    # Determine file type
                    file_type = filename.split('.')[-1].lower() if '.' in filename else None

                    files.append(AttachedFile(
                        filename=filename,
                        url=url or "",
                        size=size,
                        file_type=file_type
                    ))

                except Exception as e:
                    logger.debug(f"Error parsing file link: {e}")
                    continue

        except Exception as e:
            logger.debug(f"Error parsing attached files: {e}")

        return files

    def _find_table_value(self, page: Page, labels: List[str]) -> Optional[str]:
        """
        Find a value in a table by looking for label in th/td.
        Uses robust XPath following-sibling strategy.
        """
        for label in labels:
            try:
                # Strategy 1: strict TH -> following TD (Common in NuriJangter)
                # xpath: //th[contains(., "Label")]/following-sibling::td[1]
                xpath = f'//th[contains(., "{label}")]/following-sibling::td[1]'
                td = page.locator(xpath).first
                if td.count() > 0:
                    val = self._clean_text(td.inner_text())
                    if val: return val

                # Strategy 2: TD with class label -> following TD
                xpath = f'//td[contains(@class, "label")][contains(., "{label}")]/following-sibling::td[1]'
                td = page.locator(xpath).first
                if td.count() > 0:
                    val = self._clean_text(td.inner_text())
                    if val: return val
                
                xpath = f'//th[contains(text(), "{label}")]/following-sibling::td[1]'
                td = page.locator(xpath).first
                if td.count() > 0:
                    val = self._clean_text(td.inner_text())
                    if val: return val

                # Strategy 4: WebSquare div-based structure
                # div(label) -> following-sibling::div(value)
                # XPath: //div[contains(@class, "label") or contains(@class, "tit")][contains(., "{label}")]/following-sibling::div[1]
                xpath = f'//div[contains(., "{label}")]/following-sibling::div[1]'
                div = page.locator(xpath).first
                if div.count() > 0:
                    val = self._clean_text(div.inner_text())
                    # Filter out if it looks like another label
                    if val and len(val) < 200: 
                        return val

            except Exception as e:
                logger.debug(f"Error searching for label '{label}': {e}")
                continue

        return None

    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        if not text:
            return ""

        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()

        # Remove common label suffixes if captured by accident, though strict selectors avoid this
        text = re.sub(r'^[:\s]+', '', text)
        text = re.sub(r'[:\s]+$', '', text)

        return text
