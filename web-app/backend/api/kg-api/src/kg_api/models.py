"""Database models - matching existing PostgreSQL schema"""
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime
from sqlalchemy.sql import func

Base = declarative_base()


class User(Base):
    """User model - matches existing database schema"""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(255), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    # Note: DB uses password_hash, not hashed_password
    password_hash = Column(Text, nullable=False)
    display_name = Column(String(255), nullable=True)
    avatar_url = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now())
    is_active = Column(Boolean, default=True)

    # Relationships
    documents = relationship(
        "UserDocument", back_populates="owner", cascade="all, delete-orphan")
    favorites = relationship(
        "Favorite", back_populates="user", cascade="all, delete-orphan")
    search_history = relationship(
        "SearchHistory", back_populates="user", cascade="all, delete-orphan")


class UserDocument(Base):
    """User document model - matches existing database schema"""
    __tablename__ = "user_documents"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey(
        "users.id", ondelete="CASCADE"), nullable=False, index=True)
    title = Column(Text, nullable=False)
    content = Column(Text, nullable=True)
    doc_type = Column(String(100), default="general")
    status = Column(String(50), default="draft")  # draft, published, archived
    # JSON string of referenced provisions
    provisions = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now())

    # Relationships
    owner = relationship("User", back_populates="documents")


class Favorite(Base):
    """User favorite provisions"""
    __tablename__ = "favorites"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey(
        "users.id", ondelete="CASCADE"), nullable=False, index=True)
    provision_id = Column(String(255), nullable=False)  # Neo4j node ID
    created_at = Column(DateTime, server_default=func.now())

    # Relationships
    user = relationship("User", back_populates="favorites")


class SearchHistory(Base):
    """User search history"""
    __tablename__ = "search_history"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey(
        "users.id", ondelete="CASCADE"), nullable=False, index=True)
    query_text = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())

    # Relationships
    user = relationship("User", back_populates="search_history")
