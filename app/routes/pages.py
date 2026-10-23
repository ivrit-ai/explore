"""HTML entry points. Each returns the React shell; the client renders the page."""
from fastapi import APIRouter, Depends, Request

from ..spa import spa_index
from .auth import require_login

router = APIRouter()


@router.get('/', name='pages.home')
def home(request: Request, user_email: str = Depends(require_login)):
    analytics = request.app.state.analytics
    if analytics:
        analytics.capture_event('page_viewed', {'page': 'home'}, user_email=user_email)
    return spa_index()


@router.get('/search', name='pages.search')
def search(user_email: str = Depends(require_login)):
    return spa_index()


@router.get('/privacy', name='pages.privacy')
def privacy(request: Request):
    analytics = request.app.state.analytics
    if analytics:
        analytics.capture_event('page_viewed', {'page': 'privacy_policy'})
    return spa_index()
