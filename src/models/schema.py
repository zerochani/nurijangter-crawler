"""
Data schemas for NuriJangter bid notices.

This module defines Pydantic models for bid notices, ensuring data validation
and providing a standardized structure for storage and processing.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
from enum import Enum


class BidMethod(str, Enum):
    """Bid method types."""
    OPEN_BID = "일반경쟁"
    LIMITED_BID = "제한경쟁"
    DESIGNATED_BID = "지명경쟁"
    NEGOTIATED_BID = "수의계약"
    OTHER = "기타"


class BidStatus(str, Enum):
    """Bid status types."""
    ANNOUNCED = "공고중"
    IN_PROGRESS = "진행중"
    CLOSED = "마감"
    CANCELLED = "취소"
    UNKNOWN = "알 수 없음"


class AttachedFile(BaseModel):
    """Attached file information."""
    filename: str = Field(..., description="Name of the attached file")
    url: Optional[str] = Field(None, description="Download URL for the file")
    size: Optional[str] = Field(None, description="File size")
    file_type: Optional[str] = Field(None, description="File type/extension")

    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "filename": "입찰공고서.pdf",
                "url": "https://www.g2b.go.kr/...",
                "size": "1.2MB",
                "file_type": "pdf"
            }
        }


class BidNotice(BaseModel):
    """
    Comprehensive bid notice data model.
    """

    # --- CORE FIELDS (Requested by User) ---
    
    # 1. Identification
    bid_notice_number: str = Field(..., description="Unique bid notice number (입찰공고번호)")
    bid_notice_name: str = Field(..., description="Name/title of the bid notice (공고명)")
    announcement_agency: str = Field(..., description="Agency that announced the bid (공고기관)")
    document_number: Optional[str] = Field(None, description="Document number (문서번호)")
    
    # 2. Dates
    announcement_date: Optional[str] = Field(None, description="Date of announcement (게시일시)")
    bid_date: Optional[str] = Field(None, description="Bid start date (입찰서접수시작일시)")
    deadline_date: Optional[str] = Field(None, description="Bid deadline (입찰서접수마감일시)")
    opening_date: Optional[str] = Field(None, description="Opening date (개찰일시)")
    participation_deadline: Optional[str] = Field(None, description="Registration deadline (입찰참가자격등록마감일시)")
    
    # 3. Classification & Method
    classification: Optional[str] = Field(None, description="Bid classification (업무분류)")
    bid_method: Optional[str] = Field(None, description="Contract method (계약방법)")
    selection_method: Optional[str] = Field(None, description="Selection method (낙찰방법)")
    bid_system: Optional[str] = Field(None, description="Bidding system (입찰방식)")
    is_re_bid: Optional[str] = Field(None, description="Re-bid status (재입찰여부)")
    
    # 4. Location & Money
    opening_location: Optional[str] = Field(None, description="Opening location (개찰장소)")
    budget_amount: Optional[str] = Field(None, description="Budget amount (배정예산)")
    base_price: Optional[str] = Field(None, description="Base/Estimated price (기준금액/기초금액)")
    
    # 5. Others
    is_field_briefing_required: Optional[str] = Field(None, description="Field briefing required (현장설명회 여부)")
    
    # 6. Contact Info
    contact_person: Optional[str] = Field(None, description="Contact person (담당자)")
    contact_department: Optional[str] = Field(None, description="Department (부서)")
    phone_number: Optional[str] = Field(None, description="Phone number (담당자 전화번호)")
    email: Optional[str] = Field(None, description="Email (담당자 이메일)")

    # --- OPTIONAL / EXTENDED FIELDS ---
    
    demanding_agency: Optional[str] = Field(None, description="Agency demanding the bid")
    is_emergency: Optional[str] = Field(None, description="Emergency bid status")
    notice_type: Optional[str] = Field(None, description="Type of notice")
    process_type: Optional[str] = Field(None, description="Process type")
    
    estimated_price: Optional[str] = Field(None, description="Estimated price (legacy field)")
    pre_qualification: Optional[str] = Field(None, description="Pre-qualification requirements")
    qualification_requirements: Optional[str] = Field(None, description="Detailed qualification requirements")
    eligible_entities: Optional[str] = Field(None, description="Eligible entity types")
    
    guarantee_rate: Optional[str] = Field(None, description="Guarantee rate percentage")
    bid_bond: Optional[str] = Field(None, description="Bid bond amount or rate")
    bid_bond_deadline: Optional[str] = Field(None, description="Bid bond submission deadline")
    contract_bond: Optional[str] = Field(None, description="Contract bond amount or rate")
    
    payment_terms: Optional[str] = Field(None, description="Payment terms and conditions")
    delivery_location: Optional[str] = Field(None, description="Delivery or execution location")
    delivery_deadline: Optional[str] = Field(None, description="Delivery or completion deadline")
    contract_period: Optional[str] = Field(None, description="Contract period")
    
    fax_number: Optional[str] = Field(None, description="Contact fax number")
    
    specifications: Optional[str] = Field(None, description="Detailed specifications or scope of work")
    evaluation_criteria: Optional[str] = Field(None, description="Bid evaluation criteria")
    notes: Optional[str] = Field(None, description="Additional notes or remarks")
    
    detail_link: Optional[str] = Field(None, description="Link to detail page")
    attached_files: Optional[List[AttachedFile]] = Field(default_factory=list, description="List of attached files")
    
    status: Optional[str] = Field(None, description="Current status of the bid")
    crawled_at: datetime = Field(default_factory=datetime.now, description="Timestamp when data was crawled")
    source_url: Optional[str] = Field(None, description="Source URL of the data")
    
    additional_info: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Any additional information")

    @field_validator('announcement_date', 'deadline_date', 'bid_date', 'opening_date', mode='before')
    @classmethod
    def parse_dates(cls, v):
        """Parse and validate date fields."""
        if v is None or v == "":
            return None
        return v

    @field_validator('attached_files', mode='before')
    @classmethod
    def parse_attached_files(cls, v):
        """Parse attached files list."""
        if v is None:
            return []
        if isinstance(v, list):
            return v
        return []

    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        json_schema_extra = {
            "example": {
                "bid_notice_number": "20240101234-00",
                "bid_notice_name": "2024년도 사무용품 구매",
                "document_number": "제2024-01호",
                "is_emergency": "아니오",
                "announcement_agency": "조달청",
                "bid_method": "일반경쟁",
                "announcement_date": "2024-01-01",
                "deadline_date": "2024-01-15 14:00",
                "budget_amount": "10,000,000원",
                "contact_person": "홍길동",
                "phone_number": "02-1234-5678"
            }
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary with proper serialization."""
        return self.model_dump(mode='json', exclude_none=False)

    def to_flat_dict(self) -> Dict[str, Any]:
        """
        Convert model to flat dictionary suitable for CSV export.
        Complex nested structures are converted to strings.
        """
        data = self.model_dump(exclude_none=False)

        # Flatten attached_files
        if data.get('attached_files'):
            data['attached_files'] = '; '.join([
                f"{f.get('filename', 'N/A')} ({f.get('size', 'N/A')})"
                for f in data['attached_files']
            ])

        # Flatten additional_info
        if data.get('additional_info'):
            for key, value in data['additional_info'].items():
                data[f'extra_{key}'] = str(value)
            del data['additional_info']

        # Convert datetime to string
        if isinstance(data.get('crawled_at'), datetime):
            data['crawled_at'] = data['crawled_at'].isoformat()

        return data


class BidNoticeList(BaseModel):
    """
    Collection of bid notices with metadata.

    This model represents a collection of bid notices,
    typically from a single crawl session.
    """

    notices: List[BidNotice] = Field(default_factory=list, description="List of bid notices")
    total_count: int = Field(0, description="Total number of notices")
    crawl_started_at: datetime = Field(default_factory=datetime.now, description="When the crawl started")
    crawl_completed_at: Optional[datetime] = Field(None, description="When the crawl completed")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata about the crawl")

    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    def add_notice(self, notice: BidNotice) -> None:
        """Add a bid notice to the collection."""
        self.notices.append(notice)
        self.total_count = len(self.notices)

    def complete_crawl(self) -> None:
        """Mark the crawl as completed."""
        self.crawl_completed_at = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary."""
        return self.model_dump(mode='json')
