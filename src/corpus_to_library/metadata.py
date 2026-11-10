from typing import List, Optional
from pydantic import BaseModel, Field


class BibliographicMetadata(BaseModel):
    title: Optional[str] = Field(None, description="Main title of the document")
    subtitle: Optional[str] = Field(None, description="Subtitle if present")
    authors: List[str] = Field(default_factory=list, description="List of authors")
    editors: List[str] = Field(default_factory=list, description="List of editors")
    translators: List[str] = Field(default_factory=list, description="List of translators")
    publication_year: Optional[int] = Field(None, description="Year of publication")
    publisher: Optional[str] = Field(None, description="Publishing company or institution")
    publication_place: Optional[str] = Field(None, description="Location of publication")
    edition: Optional[str] = Field(None, description="Edition statement")
    document_type: Optional[str] = Field(None, description="Book, journal article, report, etc.")
    venue: Optional[str] = Field(None, description="Journal or conference name")
    volume: Optional[str] = Field(None, description="Journal volume number")
    issue_number: Optional[str] = Field(None, description="Journal issue number")