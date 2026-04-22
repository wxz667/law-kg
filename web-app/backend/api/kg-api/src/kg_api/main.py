"""Main FastAPI application"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from kg_api.config import settings
from kg_api.routes import auth, documents, recommendations, graph, laws, system


def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""

    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        debug=settings.DEBUG
    )

    # Configure CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # In production, specify exact origins
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(auth.router)
    app.include_router(documents.router)
    app.include_router(recommendations.router)
    app.include_router(graph.router)
    app.include_router(laws.router)
    app.include_router(system.router)

    # Health check endpoint
    @app.get("/health")
    def health_check():
        return {"status": "healthy", "version": settings.APP_VERSION}

    return app


app = create_app()
