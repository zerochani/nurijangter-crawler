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


class DetailPageParser:
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
            # Wait for specific detail content (Modal or Tab) to ensure we don't just see the background list page
            # Based on browser analysis: .w2window_content_body (Modal), .w2tabcontrol_contents_wrapper_selected (Tab), or specific ID
            try:
                page.wait_for_selector(
                    '.w2window_content_body, .w2tabcontrol_contents_wrapper_selected, div[id*="contents_content1_body"]', 
                    timeout=15000,
                    state='visible'
                )
            except:
                logger.warning("Timeout waiting for specific detail container, falling back to any table")
                try:
                    page.wait_for_selector('table', timeout=5000)
                except:
                    pass

            # Find the best context to parse (ElementHandle or Frame)
            target_context = self._find_detail_context(page)

            logger.info(f"Parsing detail page using context type: {type(target_context).__name__}")

            # Extract all table data first
            all_data = self._extract_all_table_data(target_context)
            
            # --- NEW: Extract Contact Detail Popup ---
            try:
                contact_data = self._extract_contact_popup(page)
                if contact_data:
                    logger.info(f"Extracted contact info from popup: {contact_data}")
                    all_data.update(contact_data)
            except Exception as e:
                logger.warning(f"Failed to extract contact popup: {e}")
            # ----------------------------------------

            logger.debug(f"Extracted {len(all_data)} key-value pairs from tables")

            # Map extracted data to our schema
            notice.update(self._map_to_schema(all_data))

            # Extract attached files
            attached_files = self._parse_attached_files(target_context)
            if attached_files:
                notice['attached_files'] = attached_files

            # Store source URL
            notice['source_url'] = page.url

        except Exception as e:
            logger.error(f"Failed to parse detail page: {e}", exc_info=True)

        return notice

    def _find_detail_context(self, page: Page):
        """
        Find the best context for extraction.
        Prioritizes specific detail containers (modals/tabs) to avoid parsing background list page.
        Failover to best frame or page.
        """
        # Strategy 1: Look for Modal Content Body (Standard WebSquare Modal)
        try:
            modal_content = page.query_selector('.w2window_content_body')
            if modal_content and modal_content.is_visible():
                logger.debug("Found modal content body (.w2window_content_body)")
                return modal_content
        except:
            pass

        # Strategy 2: Look for Active Tab Content (Specific to Tabbed Details)
        try:
            tab_content = page.query_selector('.w2tabcontrol_contents_wrapper_selected')
            if tab_content and tab_content.is_visible():
                logger.debug("Found active tab content (.w2tabcontrol_contents_wrapper_selected)")
                return tab_content
        except:
            pass
            
        # Strategy 3: Specific ID for some layouts (as found in investigation)
        try:
            specific_container = page.query_selector('div[id*="contents_content1_body"]')
            if specific_container and specific_container.is_visible():
                logger.debug("Found specific content container (content1_body)")
                return specific_container
        except:
            pass

        # Strategy 4: Fallback to Best Frame (Original Logic)
        return self._find_best_frame(page)

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
                    # CHECK: Is this TH inside the Search Filter?
                    # The Search Filter causes "garbage" data (e.g. date picker ranges)
                    is_search_filter = th.evaluate("el => el.closest('#mf_wfm_container_shcBidPbanc, #mf_wfm_container_grpSrchBox, .sh_group') !== null")
                    if is_search_filter:
                        continue

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
                # CHECK: Is this table inside Search Filter?
                try:
                    if table.evaluate("el => el.closest('#mf_wfm_container_shcBidPbanc, #mf_wfm_container_grpSrchBox, .sh_group') !== null"):
                        continue
                except:
                    pass

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
            # Look for .label or .th class followed by .value or .td class
            label_elements = frame.query_selector_all('.label, .th, span.label, div.label')
            for label_el in label_elements:
                try:
                    # CHECK: Is this Label inside Search Filter?
                    if label_el.evaluate("el => el.closest('#mf_wfm_container_shcBidPbanc, #mf_wfm_container_grpSrchBox, .sh_group') !== null"):
                        continue

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
        # IMPORTANT: More specific labels first to avoid false matches
        field_mappings = {
            'classification': ['업무분류', '분류', '물품분류', '용역분류', '공고분류'],
            'document_number': ['문서번호'],
            'is_emergency': ['긴급입찰여부'],
            'notice_type': ['공고종류'],
            'process_type': ['공고처리구분'],
            'bid_system': ['입찰방식'],
            'is_re_bid': ['재입찰여부'],
            'demanding_agency': ['수요기관', '발주기관'],
            'opening_location': ['개찰장소'],
            'is_field_briefing_required': ['현장설명', '현장설명회', '현장설명여부'],
            'bid_method': ['계약방법', '입찰방법'],
            'selection_method': ['낙찰방법', '낙찰자결정방법'],
            'budget_amount': ['배정예산액', '배정예산', '예산금액', '추정금액'],
            'base_price': ['기준금액', '기초금액', '시작가격', '예정가격'],
            'estimated_price': ['추정가격', '예정가격'],
            'pre_qualification': ['사전규격', '사전심사', '적격심사대상여부'],
            'qualification_requirements': ['지역제한', '업종제한', '자격요건'],  # More specific
            'guarantee_rate': ['보증금율', '계약보증금율'],
            'bid_bond': ['입찰보증금', '입찰보증'],
            'contract_bond': ['계약보증금', '계약이행보증', '이행보증금'],
            'payment_terms': ['대금지급방법', '계약금지급', '대금지급조건'],
            'delivery_location': ['개찰장소', '납품장소', '계약장소', '이행장소'],  # '개찰장소' added
            'delivery_deadline': ['납품기한', '이행기간', '납품기간'],
            'contract_period': ['계약기간'],
            'contact_person': ['담당자'],  # More specific - exact match only
            'contact_department': ['담당부서', '부서', '소속'],
            'phone_number': ['전화번호', '연락처 전화번호'],  # More specific
            'fax_number': ['팩스', 'FAX', '팩스번호'],
            'email': ['이메일', 'E-mail', '전자우편'],
            'bid_date': ['입찰서접수시작일시', '입찰일시', '입찰개시일시'],
            'opening_date': ['개찰일시', '개찰일'],
            'bid_bond_deadline': ['입찰보증서접수마감일시'],
            'participation_deadline': ['입찰참가자격등록마감일시', '참가자격등록마감일시'],
            'specifications': ['세부규격', '과업내용', '규격', '내역'],
            'evaluation_criteria': ['낙찰방법', '평가기준', '낙찰기준', '낙찰자결정방법', '적격심사표'],
            'eligible_entities': ['지사/지점허용여부', '참가자격', '적격업체'],
            'notes': ['개찰및낙찰-비고', '비고', '특이사항', '참고사항'],
            'status': ['상태', '진행상태', '진행현황']
        }

        # Blacklisted labels that should never match
        blacklisted_labels = ['공고처리상태', '검색', '정렬', '보기']

        # Fuzzy match each field
        for schema_field, possible_labels in field_mappings.items():
            value = self._find_value_by_labels(raw_data, possible_labels, blacklisted_labels)
            if value:
                # Special cleaning for opening_date
                if schema_field == 'opening_date':
                    value = self._clean_opening_date(value)

                # Validate the value makes sense for this field
                if self._validate_field_value(schema_field, value):
                    mapped[schema_field] = value
                else:
                    logger.debug(f"Skipped invalid value for {schema_field}: {value[:50]}")

        return mapped

    def _validate_field_value(self, field_name: str, value: str) -> bool:
        """
        Validate that the extracted value makes sense for the given field.

        Args:
            field_name: Schema field name
            value: Extracted value

        Returns:
            True if value is valid for this field, False otherwise
        """
        if value is None:
            return True

        if field_name == 'opening_date':
            # Check length to filter out garbage data (e.g., long list of years from search filter)
            if len(str(value)) > 50:
                logger.warning(f"Likely garbage data detected for opening_date (length {len(str(value))}). Rejected.")
                return False

        import re
        
        # Don't allow dates in non-date fields
        date_fields = ['announcement_date', 'deadline_date', 'bid_date', 'opening_date', 'delivery_deadline', 
                      'bid_bond_deadline', 'participation_deadline']
        if field_name not in date_fields:
            # Check if value looks like a date (YYYY/MM/DD or similar)
            if re.search(r'\d{4}[/-]\d{2}[/-]\d{2}', value):
                return False

        # Don't allow names in phone/fax/email fields
        contact_fields = ['phone_number', 'fax_number', 'email']
        if field_name in contact_fields:
            # If value doesn't contain digits, it's probably not a phone/fax
            # If no @ symbol, it's probably not an email
            if field_name == 'phone_number' and not re.search(r'\d', value):
                return False
            if field_name == 'fax_number' and not re.search(r'\d', value):
                return False
            if field_name == 'email' and '@' not in value:
                return False

        # Don't allow very short values (likely parsing errors)
        if len(value.strip()) < 2:
            return False

        # Specific validation for status
        if field_name == 'status':
            if '게시미게시' in value or '검색' in value:
                return False

        return True

    def _find_value_by_labels(self, data: Dict[str, str], labels: List[str], blacklist: List[str] = None) -> Optional[str]:
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

                # Check blacklist
                if blacklist and any(b in normalized_key for b in blacklist):
                    continue

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

    def _clean_opening_date(self, text: str) -> str:
        """
        Clean opening_date field which often contains calendar widget text.
        Extracts only the date and time pattern (YYYY/MM/DD HH:MM).
        """
        if not text:
            return ""
            
        # Strategy 1: strict date<space>time pattern
        match = re.search(r'(\d{4}[/-]\d{2}[/-]\d{2})\s*(\d{2}:\d{2})', text)
        if match:
            return f"{match.group(1)} {match.group(2)}"

        # Strategy 2: find date and time separately (if garbage in between)
        date_match = re.search(r'(\d{4}[/-]\d{2}[/-]\d{2})', text)
        time_match = re.search(r'(\d{2}:\d{2})', text)
        
        if date_match and time_match:
             return f"{date_match.group(1)} {time_match.group(1)}"
            
        return text

    def _extract_contact_popup(self, page: Page) -> Dict[str, str]:
        """
        Click the 'Detail View' button to open contact officer popup and extract phone/email.
        
        Selectors identified:
        - Button ID: #mf_wfm_container_btnUsrDtail
        - Popup Close ID: #mf_wfm_container_BidPbancUsrP_close
        - Phone XPath: //th[.//label[contains(text(), '연락처')]]/following-sibling::td//span
        - Email XPath: //th[.//label[contains(text(), '이메일')]]/following-sibling::td//span
        """
        data = {}
        
        try:
            # 1. Find and Click Button
            # Try specific ID first, then generic '상세보기' button
            button_selector = '#mf_wfm_container_btnUsrDtail'
            button = page.query_selector(button_selector)
            
            if not button or not button.is_visible():
                # Fallback: Find button with text '상세보기' next to '입찰담당정보'
                logger.debug("Specific detail button not found, trying generic search...")
                # Simple heuristic: Any button with '상세보기' text
                button = page.get_by_text("상세보기", exact=False).first
            
            if button and button.is_visible():
                logger.debug("Found 'Contact Detail' button, clicking...")
                button.click()
                
                # 2. Wait for Popup
                # Wait for any typical popup container or specific close button
                try:
                    page.wait_for_selector('#mf_wfm_container_BidPbancUsrP_close, .w2window_content_body', timeout=3000)
                    # Small sleep to ensure text rendered
                    page.wait_for_timeout(500)
                except:
                    logger.warning("Popup did not appear after clicking detail button")
                    return {}

                # 3. Extract Data using robust XPath
                # Helper to extract text via XPath
                def get_text_by_xpath(xpath):
                    try:
                        el = page.query_selector(f"xpath={xpath}")
                        return self._clean_text(el.inner_text()) if el else None
                    except:
                        return None
                
                phone = get_text_by_xpath("//th[.//label[contains(text(), '연락처')]]/following-sibling::td")
                email = get_text_by_xpath("//th[.//label[contains(text(), '이메일')]]/following-sibling::td")
                
                if phone:
                    # Clean up: sometimes contains / or multiple numbers. Take first valid one? 
                    # For now just save raw cleaned text.
                    data['전화번호'] = phone
                if email:
                    data['이메일'] = email
                
                logger.debug(f"Popup extraction result: {data}")

                # 4. Close Popup
                try:
                    close_btn = page.query_selector('#mf_wfm_container_BidPbancUsrP_close')
                    if close_btn:
                        close_btn.click()
                    else:
                        # Fallback close
                        page.keyboard.press('Escape')
                except Exception as e:
                    logger.warning(f"Failed to close popup: {e}")

        except Exception as e:
            logger.debug(f"Error in contact popup extraction: {e}")
            # Attempt to recover by pressing Escape just in case popup is stuck open
            try:
                page.keyboard.press('Escape')
            except:
                pass
            
        return data
