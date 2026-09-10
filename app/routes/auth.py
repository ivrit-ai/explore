"""Authentication against the platform's built-in Google sign-in.

The platform runs the Google dance itself on reserved ``/xhost-auth/*`` paths
and hands the result back as ``__Host-xhost_id``: an RS256-signed JWT. It never
tells the app who is calling — the app verifies that cookie and decides access
itself, which is what happens here.

Verification pins RS256 and picks the key by the token's ``kid``, rather than
trusting the token's own ``alg`` header, which is what stops an
algorithm-confusion forgery. Issuer, audience and expiry are all checked. A
decode-only helper would accept a forged token, so a real verifying library
does the work.
"""

from fastapi import APIRouter, Request
from starlette.responses import RedirectResponse
from urllib.parse import quote, urlencode
from ..templating import render
import logging
import os

router = APIRouter()
logger = logging.getLogger(__name__)

XHOST_ISSUER = "https://auth.xhostd.com"
XHOST_JWKS_URL = "https://auth.xhostd.com/xhost-auth/jwks"
XHOST_COOKIE = "__Host-xhost_id"          # reserved; never set this ourselves
XHOST_LOGIN = "/xhost-auth/login"
XHOST_LOGOUT = "/xhost-auth/logout"

_jwks_client = None


class LoginRequired(Exception):
    """Raised when a route requires login but the user is not authenticated."""
    pass


def _jwks():
    """The platform's signing keys, fetched once and cached by the client."""
    global _jwks_client
    if _jwks_client is None:
        from jwt import PyJWKClient

        _jwks_client = PyJWKClient(XHOST_JWKS_URL, cache_keys=True)
    return _jwks_client


def _audiences(request: Request) -> list[str]:
    """Hostnames this deployment accepts tokens for.

    The platform mints a token for one exact hostname, so a custom domain needs
    listing in EXPLORE_AUTH_AUDIENCES. Without that we fall back to the host
    this request arrived on, which the platform's own routing determines.
    """
    configured = os.environ.get("EXPLORE_AUTH_AUDIENCES", "").strip()
    if configured:
        return [h.strip() for h in configured.split(",") if h.strip()]
    host = request.headers.get("host", "")
    return [host.split(":")[0]] if host else []


def identity(request: Request) -> dict | None:
    """The verified caller, or None when there is no valid session.

    Returns the token's claims, in which ``sub`` is the stable identifier and
    ``email`` may change over time.
    """
    if os.environ.get("APP_ENV") == "development" and os.environ.get("TS_USER_EMAIL"):
        email = os.environ["TS_USER_EMAIL"]
        return {"sub": f"dev:{email}", "email": email, "name": "dev"}

    token = request.cookies.get(XHOST_COOKIE)
    if not token:
        return None

    audiences = _audiences(request)
    if not audiences:
        logger.error("Cannot verify identity: no audience configured and no Host header")
        return None

    import jwt

    try:
        key = _jwks().get_signing_key_from_jwt(token).key
        return jwt.decode(
            token,
            key,
            algorithms=["RS256"],          # pinned; never taken from the token
            issuer=XHOST_ISSUER,
            audience=audiences,
            options={"require": ["exp", "iss", "aud", "sub"]},
        )
    except Exception as exc:
        # An expired or malformed cookie is a normal logged-out state, not a fault.
        logger.info("Identity cookie rejected: %s: %s", type(exc).__name__, exc)
        return None


def require_login(request: Request) -> str:
    """FastAPI dependency that enforces authentication.

    Returns the user's email address.
    """
    claims = identity(request)
    if claims is None:
        raise LoginRequired()
    request.state.user_sub = claims.get("sub")
    return claims.get("email") or claims["sub"]


def login_url(return_to: str = "/") -> str:
    """Platform sign-in, coming back to `return_to` afterwards."""
    return f"{XHOST_LOGIN}?{urlencode({'return_to': return_to})}"


@router.get("/login", name="auth.login")
def login(request: Request):
    """Branded landing page whose button starts the platform sign-in."""
    analytics = request.app.state.analytics
    if analytics:
        analytics.capture_event('page_viewed', {'page': 'login'})

    google_analytics_tag = os.environ.get("GOOGLE_ANALYTICS_TAG", "")

    return render(request, "login.html", google_analytics_tag=google_analytics_tag)


@router.get("/authorize", name="auth.authorize")
def authorize(request: Request):
    """Hand off to the platform's Google sign-in."""
    next_url = request.query_params.get("next") or "/"
    if not next_url.startswith("/"):        # never redirect off-site
        next_url = "/"
    return RedirectResponse(url=login_url(next_url))


@router.get("/logout", name="auth.logout")
def logout(request: Request):
    """Clear the platform session; there is no app-side session to drop."""
    analytics = request.app.state.analytics
    claims = identity(request)
    if analytics and claims:
        analytics.capture_event('logout', {'email': claims.get("email")})

    return RedirectResponse(url=f"{XHOST_LOGOUT}?{urlencode({'return_to': '/'})}")
