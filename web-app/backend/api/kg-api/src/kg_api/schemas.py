"""Pydantic schemas for request/response validation"""
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from datetime import datetime


# User Schemas
class UserBase(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    display_name: Optional[str] = None


class UserCreate(UserBase):
    password: str = Field(..., min_length=6)


class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None


class UserOut(UserBase):
    id: int
    avatar_url: Optional[str] = None
    created_at: datetime
    is_active: bool

    class Config:
        from_attributes = True


class UserLogin(BaseModel):
    username: str = Field(..., description="Username or email")
    password: str = Field(..., min_length=1)


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: int
    username: str


# Document Schemas
class UserDocumentBase(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    content: Optional[str] = None
    doc_type: Optional[str] = "通用文书"
    status: Optional[str] = "draft"


class UserDocumentCreate(UserDocumentBase):
    pass


class UserDocumentUpdate(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None
    doc_type: Optional[str] = None
    status: Optional[str] = None


class UserDocumentOut(UserDocumentBase):
    id: int
    user_id: int
    status: str
    provisions: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
