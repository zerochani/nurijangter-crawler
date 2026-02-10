"""
누리장터 입찰 공고 상세 페이지 파서 개선판 (Improved Detail Page Parser)

WebSquare 기반 페이지에서 데이터를 추출하기 위한 강력한 전략들을 포함하고 있습니다.
"""

from typing import Dict, List, Any, Optional
from playwright.sync_api import Page, Frame
import logging
import re

from ..models.schema import AttachedFile

logger = logging.getLogger(__name__)


class DetailPageParser:
    """
    누리장터 상세 페이지를 위한 다중 추출 전략 파서.

    전략(Strategies):
    1. XPath 기반 TH-TD 형제 요소 찾기 (XPath-based TH-TD following-sibling)
    2. 테이블 행 순회 및 유연한 매칭 (Table row scanning)
    3. 라벨-값 클래스 매칭 및 퍼지 매칭 (Label-value pair extraction)
    4. 프레임 인식 파싱 (Frame-aware parsing)
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize improved detail page parser."""
        self.config = config
        self.detail_fields = config.get('extraction', {}).get('detail_fields', [])

    def parse_page(self, page: Page, base_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        개선된 추출 로직으로 상세 페이지를 파싱합니다.

        Args:
            page: Playwright 페이지 객체 (또는 새 탭)
            base_data: 목록 페이지에서 가져온 기본 데이터

        Returns:
            완전한 입찰 공고 데이터 딕셔너리
        """
        notice = base_data.copy()

        try:
            # Wait for specific detail content (Modal or Tab) to ensure we don't just see the background list page
            # 특정 상세 컨텐츠(모달 또는 탭)가 로드될 때까지 대기하여 백그라운드 목록 페이지를 파싱하는 것을 방지합니다.
            # .w2window_content_body (모달), .w2tabcontrol_contents_wrapper_selected (탭) 등
            try:
                page.wait_for_selector(
                    '.w2window_content_body, .w2tabcontrol_contents_wrapper_selected, div[id*="contents_content1_body"]', 
                    timeout=15000,
                    state='visible'
                )
            except:
                logger.warning("상세 컨텐츠 대기 시간 초과, 일반 테이블 대기로 대체합니다")
                try:
                    page.wait_for_selector('table', timeout=5000)
                except:
                    pass

            # Find the best context to parse (ElementHandle or Frame)
            # 파싱할 최적의 컨텍스트 찾기 (ElementHandle 또는 Frame)
            target_context = self._find_detail_context(page)

            logger.info(f"파싱 컨텍스트 타입: {type(target_context).__name__}")

            # Extract all table data first
            # 모든 테이블 데이터 우선 추출
            all_data = self._extract_all_table_data(target_context)
            
            # ----------------------------------------

            logger.debug(f"테이블에서 {len(all_data)}개의 키-값 쌍을 추출했습니다")

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
        추출을 위한 최적의 컨텍스트를 찾습니다.
        백그라운드 목록 페이지 파싱을 피하기 위해 특정 상세 컨테이너(모달/탭)를 우선시합니다.
        실패 시 최적의 프레임 또는 페이지로 폴백합니다.
        """
        # Strategy 1: Look for Modal Content Body (Standard WebSquare Modal)
        # 전략 1: 모달 컨텐츠 바디 찾기 (표준 WebSquare 모달)
        try:
            modal_content = page.query_selector('.w2window_content_body')
            if modal_content and modal_content.is_visible():
                logger.debug("모달 컨텐츠 발견 (.w2window_content_body)")
                return modal_content
        except:
            pass

        # Strategy 2: Look for Active Tab Content (Specific to Tabbed Details)
        # 전략 2: 활성 탭 컨텐츠 찾기 (탭 방식 상세 페이지용)
        try:
            tab_content = page.query_selector('.w2tabcontrol_contents_wrapper_selected')
            if tab_content and tab_content.is_visible():
                logger.debug("활성 탭 컨텐츠 발견 (.w2tabcontrol_contents_wrapper_selected)")
                return tab_content
        except:
            pass
            
        # Strategy 3: Specific ID for some layouts (as found in investigation)
        # 전략 3: 특정 레이아웃 ID 찾기
        try:
            specific_container = page.query_selector('div[id*="contents_content1_body"]')
            if specific_container and specific_container.is_visible():
                logger.debug("특정 컨텐츠 컨테이너 발견 (content1_body)")
                return specific_container
        except:
            pass

        # Strategy 4: Fallback to Best Frame (Original Logic)
        # 전략 4: 최적 프레임으로 폴백
        return self._find_best_frame(page)

    def _find_best_frame(self, page: Page):
        """
        가장 많은 컨텐츠(메인 컨텐츠일 가능성이 높음)를 가진 프레임을 찾습니다.

        Returns:
            Page 또는 Frame 객체
        """
        frames = page.frames

        if len(frames) == 1:
            return page

        # Score each frame by table count
        # 테이블 개수로 각 프레임 점수 매기기
        best_frame = page
        max_score = len(page.query_selector_all('table'))

        for frame in frames:
            try:
                table_count = len(frame.query_selector_all('table'))
                if table_count > max_score:
                    max_score = table_count
                    best_frame = frame
                    logger.debug(f"프레임 선택됨 '{frame.name or 'unnamed'}' (테이블 {table_count}개)")
            except:
                continue

        return best_frame

    def _extract_all_table_data(self, frame) -> Dict[str, str]:
        """
        다중 전략을 사용하여 모든 테이블에서 키-값 쌍을 추출합니다.

        Returns:
            라벨 -> 값 딕셔너리
        """
        data = {}

        # Strategy 1: XPath - TH with following TD
        # 전략 1: XPath - TH 다음에 오는 TD 찾기
        try:
            # Find all TH elements
            ths = frame.query_selector_all('th')
            for th in ths:
                try:
                    # CHECK: Is this TH inside the Search Filter?
                    # 체크: 이 TH가 검색 필터 안에 있는지? (검색 필터는 잘못된 데이터를 유발함)
                    is_search_filter = th.evaluate("el => el.closest('#mf_wfm_container_shcBidPbanc, #mf_wfm_container_grpSrchBox, .sh_group, .search_box, .search_area, .tbl_search') !== null")
                    if is_search_filter:
                        continue

                    label = self._clean_text(th.inner_text())
                    if not label:
                        continue

                    # Try to find following TD using JS
                    # JS를 사용하여 다음 TD 찾기
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
                            # Ignore garbage values (too long)
                            # 너무 긴 값(쓰레기 데이터) 무시
                            if len(value) > 100:
                                logger.debug(f"[XPath] 긴 값 건너뜀 {label}: {len(value)}자")
                            else:
                                data[label] = value
                                logger.debug(f"[XPath] {label} = {value[:50]}")
                except Exception as e:
                    logger.debug(f"TH에서 추출 실패: {e}")
                    continue
        except Exception as e:
            logger.debug(f"전략 1 실패: {e}")

        # Strategy 2: Table row iteration
        # 전략 2: 테이블 행 순회
        try:
            tables = frame.query_selector_all('table')
            for table in tables:
                # CHECK: Is this table inside Search Filter?
                try:
                    if table.evaluate("el => el.closest('#mf_wfm_container_shcBidPbanc, #mf_wfm_container_grpSrchBox, .sh_group, .search_box, .search_area, .tbl_search') !== null"):
                        continue
                except:
                    pass

                rows = table.query_selector_all('tr')
                # DEBUG: Log first row of each table to see if we're in the right place
                if rows:
                    try:
                        first_row_text = rows[0].evaluate("el => el.innerText").replace('\n', ' ')
                        logger.info(f"DEBUG TABLE: Found table with {len(rows)} rows. Row 1: {first_row_text[:100]}...")
                    except: pass

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
                                class1 = cell1.evaluate("el => el.className")

                                # TH followed by TD (or TD.w2tb_th followed by TD)
                                # TH 다음에 TD가 오는지 확인 (또는 class에 w2tb_th가 있는 TD)
                                is_header = (tag1 == "TH") or (tag1 == "TD" and "w2tb_th" in class1)
                                
                                # DEBUG LOGGING
                                raw_text = self._clean_text(cell1.inner_text())
                                if "개찰일시" in raw_text:
                                    logger.info(f"디버그 전략 2: '개찰일시' 후보 발견. Tag1={tag1}, Class1='{class1}', IsHeader={is_header}, Tag2={tag2}")

                                if is_header and tag2 == "TD":
                                    label = raw_text
                                    value = self._clean_text(cell2.inner_text())
                                    
                                    if "개찰일시" in label:
                                         logger.info(f"디버그 전략 2: '개찰일시' 추출됨 = '{value}'")

                                    if label and value:
                                        # Ignore garbage values (too long)
                                        # 긴 값(쓰레기 데이터) 무시
                                        if len(value) > 100:
                                             logger.debug(f"[Table] 긴 값 건너뜀 {label}: {len(value)}자")
                                        elif label not in data:  # Don't overwrite
                                            data[label] = value
                                            logger.debug(f"[Table] {label} = {value[:50]}")

                                    i += 2
                                else:
                                    i += 1
                    except Exception as e:
                        logger.info(f"행 파싱 실패: {e}")
                        continue
        except Exception as e:
            logger.debug(f"전략 2 실패: {e}")

        # Strategy 3: Label-value divs/spans (less common in tables but worth trying)
        # 전략 3: Label-Value div/span (테이블이 아닌 div 구조)
        try:
            # Look for .label or .th class followed by .value or .td class
            # Include 'label' tag
            label_elements = frame.query_selector_all('.label, .th, span.label, div.label, label')
            for label_el in label_elements:
                try:
                    # CHECK: Is this Label inside Search Filter?
                    if label_el.evaluate("el => el.closest('#mf_wfm_container_shcBidPbanc, #mf_wfm_container_grpSrchBox, .sh_group, .search_box, .search_area, .tbl_search') !== null"):
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
                                           sibling.classList.contains('w2tb_td') ||
                                           sibling.tagName === 'TD')) {
                                return sibling;
                            }

                            // Try parent's next child (sibling of label)
                            let parent = el.parentElement;
                            if (parent) {
                                let children = Array.from(parent.children);
                                let idx = children.indexOf(el);
                                if (idx >= 0 && idx < children.length - 1) {
                                    return children[idx + 1];
                                }
                                
                                // NEW: Try Parent's Next Sibling (TD -> TD)
                                // If label is inside a TD/TH, the value might be in the next TD
                                if (parent.tagName === 'TD' || parent.tagName === 'TH') {
                                    let parentSibling = parent.nextElementSibling;
                                    if (parentSibling && parentSibling.tagName === 'TD') {
                                        return parentSibling;
                                    }
                                }
                            }

                            return null;
                        }
                    """)

                    if value_el:
                        value = self._clean_text(value_el.evaluate("el => el.innerText"))
                        if value:
                             if len(value) > 100:
                                 logger.debug(f"[Div] Skipped long value for {label}")
                             elif label not in data:
                                data[label] = value
                                logger.debug(f"[Div] {label} = {value[:50]}")
                except:
                    continue
        except Exception as e:
            logger.debug(f"Strategy 3 failed: {e}")

        return data

    def _map_to_schema(self, raw_data: Dict[str, str]) -> Dict[str, Any]:
        """
        추출된 원시 데이터를 BidNotice 스키마 필드에 매핑합니다.

        한국어 라벨의 다양한 변형을 처리하기 위해 퍼지 매칭(Fuzzy Matching)을 사용합니다.
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
        추출된 값이 해당 필드에 적절한지 검증합니다.

        Args:
            field_name: 스키마 필드명
            value: 추출된 값

        Returns:
            유효하면 True, 아니면 False
        """
        if value is None:
            return True

        if field_name == 'opening_date':
            # Check length to filter out garbage data (e.g., long list of years from search filter)
            if len(str(value)) > 50:
                logger.warning(f"Likely garbage data detected for opening_date (length {len(str(value))}). Content start: {str(value)[:200]}...")
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
        여러 라벨 변형을 시도하여 데이터에서 값을 찾습니다.

        다음과 같은 경우를 처리하기 위해 퍼지 매칭을 사용합니다:
        - 불필요한 공백
        - 끝부분의 콜론(:)
        - 괄호가 포함된 단위
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
        상세 페이지에서 첨부파일을 추출합니다.

        누리장터는 보통 'grdFile' 등이 포함된 ID의 그리드를 사용합니다.
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
        추출된 텍스트를 정제합니다.

        처리 내용:
        - 여분의 공백 제거
        - 개행 문자 제거
        - 일반적인 라벨 접미사 제거
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
        Extracts only the date and time pattern.
        """
        if not text:
            return ""
            
        # Common usage: "2026/02/10 10:00" or "2026-02-10 10:00"
        match = re.search(r'(\d{4}[/-]\d{2}[/-]\d{2})\s*(\d{2}:\d{2})', text)
        if match:
            return f"{match.group(1)} {match.group(2)}"

        # Korean format: "2026년 02월 10일 10:00"
        kor_match = re.search(r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)\s*(\d{2}:\d{2})', text)
        if kor_match:
             return f"{kor_match.group(1)} {kor_match.group(2)}"

        # Strategy 2: find date and time separately (if garbage in between)
        date_match = re.search(r'(\d{4}[/-]\d{2}[/-]\d{2})', text)
        time_match = re.search(r'(\d{2}:\d{2})', text)
        
        if date_match and time_match:
             return f"{date_match.group(1)} {time_match.group(1)}"
            
        return text

    def extract_contact_popup(self, page: Page) -> Dict[str, Any]:
        """
        Extract contact information from the manager popup.
        
        Args:
            page: Playwright page object (already switched to popup context if needed)
            
        Returns:
            Dictionary with contact info (manager_phone, manager_email)
        """
        data = {}
        try:
            logger.info("Extracting data from Manager Contact Popup...")
            
            # Browser subagent identified robust XPath with data-title attribute
            # These are more stable than label-based searches
            
            # Phone: //td[@data-title='연락처']/span
            phone_elem = page.locator("xpath=//td[@data-title='연락처']/span").first
            if phone_elem.count() > 0:
                text = self._clean_text(phone_elem.inner_text())
                if text:
                    data['manager_phone'] = text
                    logger.info(f"Found phone: {text}")

            # Email: //td[@data-title='이메일']/span
            email_elem = page.locator("xpath=//td[@data-title='이메일']/span").first
            if email_elem.count() > 0:
                text = self._clean_text(email_elem.inner_text())
                if text:
                    data['manager_email'] = text
                    logger.info(f"Found email: {text}")

            if not data:
                logger.warning("No contact data found in popup (selectors matched nothing)")
                # Log page content specific to popup frame/modal if possible
                try:
                    poup_content = page.locator(".w2popup_window").last.inner_html()
                    logger.info(f"Popup content snapshot: {poup_content[:500]}...")
                except: pass

        except Exception as e:
            logger.warning(f"Failed to extract contact popup data: {e}")
            
        return data
