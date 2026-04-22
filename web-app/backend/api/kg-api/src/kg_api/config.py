"""Application configuration and settings"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # Application
    APP_NAME: str = "Legal Knowledge Graph API"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = True

    # Database
    DATABASE_URL: str = "postgresql://postgres:change_this_password@localhost:5432/law_kg"

    # Neo4j
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "change_this_password"

    # Elasticsearch
    ELASTICSEARCH_HOST: str = "http://localhost:9200"

    # AI Recommendation (Qwen OpenAI-compatible)
    AI_PROVIDER: str = "qwen"
    QWEN_API_KEY: str = ""
    QWEN_MODEL: str = "qwen-plus"
    QWEN_BASE_URL: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"

    # Recommendation
    RECOMMENDATION_CACHE_TTL: int = 300

    # JWT Authentication
    SECRET_KEY: str = "your-secret-key-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
