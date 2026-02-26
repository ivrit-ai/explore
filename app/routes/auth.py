from fastapi import APIRouter, Request, Depends
from starlette.responses import RedirectResponse
from authlib.integrations.starlette_client import OAuth
from ..templating import render
import os

router = APIRouter()

oauth = OAuth()


class LoginRequired(Exception):
    """Raised when a route requires login but the user is not authenticated."""
    pass


def require_login(request: Request) -> str:
    """FastAPI dependency that enforces authentication.
    Returns the user's email address.
    """
    dev_mode = os.environ.get('APP_ENV') == 'development'
    if dev_mode and os.environ.get('TS_USER_EMAIL'):
        request.session['user_email'] = os.environ['TS_USER_EMAIL']

    if 'user_email' not in request.session:
        raise LoginRequired()

    return request.session['user_email']


def init_oauth(app):
    """Initialize OAuth with the FastAPI app (called in production only)."""
    oauth.register(
        name='google',
        client_id=os.environ.get('GOOGLE_CLIENT_ID'),
        client_secret=os.environ.get('GOOGLE_CLIENT_SECRET'),
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={'scope': 'openid email profile'},
    )
    return oauth


@router.get("/login", name="auth.login")
def login(request: Request):
    analytics = request.app.state.analytics
    if analytics:
        analytics.capture_event('page_viewed', {'page': 'login'})

    google_analytics_tag = os.environ.get("GOOGLE_ANALYTICS_TAG", "")

    return render(request, "login.html", google_analytics_tag=google_analytics_tag)


@router.get("/authorize", name="auth.authorize")
async def authorize(request: Request):
    if 'next_url' not in request.session:
        request.session['next_url'] = str(request.url_for('main.home'))

    google = oauth.create_client('google')
    redirect_uri = str(request.url_for('auth.authorized'))
    return await google.authorize_redirect(request, redirect_uri)


@router.get("/login/authorized", name="auth.authorized")
async def authorized(request: Request):
    google = oauth.create_client('google')
    token = await google.authorize_access_token(request)

    if token is None:
        error_reason = request.query_params.get("error_reason", "Unknown")
        error_desc = request.query_params.get("error_description", "Unknown")
        error_message = f"Access denied: reason={error_reason} error={error_desc}"

        analytics = request.app.state.analytics
        if analytics:
            analytics.capture_event('login_failed', {'reason': error_message})

        return RedirectResponse(url=str(request.url_for('auth.login')))

    # Get user info from the ID token
    user_info = token.get('userinfo')
    if user_info is None:
        # Fallback: fetch from userinfo endpoint
        resp = await google.get('https://www.googleapis.com/oauth2/v1/userinfo')
        user_info = resp.json()

    request.session['user_email'] = user_info['email']

    analytics = request.app.state.analytics
    if analytics:
        analytics.capture_event('login_successful', {'email': request.session['user_email']})

    next_url = request.session.pop('next_url', str(request.url_for('main.home')))

    return RedirectResponse(url=next_url)


@router.get("/logout", name="auth.logout")
def logout(request: Request):
    analytics = request.app.state.analytics
    if analytics and 'user_email' in request.session:
        analytics.capture_event('logout', {'email': request.session['user_email']})

    request.session.pop('user_email', None)

    return RedirectResponse(url=str(request.url_for('main.home')))
