import os
from functools import wraps

from flask import Blueprint, current_app, redirect, render_template, request, session, url_for
from flask_oauthlib.client import OAuth

bp = Blueprint("auth", __name__)

oauth = OAuth()


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Skip authentication in development mode if TS_USER_EMAIL is set
        in_dev = os.environ.get("FLASK_ENV") == "development"
        if in_dev and os.environ.get("TS_USER_EMAIL"):
            session["user_email"] = os.environ["TS_USER_EMAIL"]

        if "user_email" not in session:
            # Store the requested URL in session
            session["next_url"] = request.url
            return redirect(url_for("auth.login"))
        return f(*args, **kwargs)

    return decorated_function


def init_oauth(app):
    """Initialize OAuth with the Flask app"""
    global oauth

    # Configure Google OAuth
    google = oauth.remote_app(
        "google",
        consumer_key=os.environ.get("GOOGLE_CLIENT_ID"),
        consumer_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
        request_token_params={"scope": "email"},
        base_url="https://www.googleapis.com/oauth2/v1/",
        request_token_url=None,
        access_token_method="POST",
        access_token_url="https://accounts.google.com/o/oauth2/token",
        authorize_url="https://accounts.google.com/o/oauth2/auth",
    )

    # Register token getter for Google OAuth
    @google.tokengetter
    def get_google_oauth_token():
        return session.get("google_token")

    return google


@bp.route("/login")
def login():
    # Track page view
    analytics = current_app.config.get("ANALYTICS_SERVICE")
    if analytics:
        analytics.capture_event("page_viewed", {"page": "login"})

    # Get Google Analytics tag from environment if available
    google_analytics_tag = os.environ.get("GOOGLE_ANALYTICS_TAG", "")

    return render_template("login.html", google_analytics_tag=google_analytics_tag)


@bp.route("/authorize")
def authorize():
    # Store the next URL if not already in session
    if "next_url" not in session:
        session["next_url"] = url_for("main.home")

    google = current_app.extensions.get("google_oauth")
    return google.authorize(callback=url_for("auth.authorized", _external=True))


@bp.route("/login/authorized")
def authorized():
    google = current_app.extensions.get("google_oauth")
    resp = google.authorized_response()

    if resp is None or resp.get("access_token") is None:
        error_message = "Access denied: reason={} error={}".format(
            request.args.get("error_reason", "Unknown"), request.args.get("error_description", "Unknown")
        )

        # Track failed login
        analytics = current_app.config.get("ANALYTICS_SERVICE")
        if analytics:
            analytics.capture_event("login_failed", {"reason": error_message})

        return redirect(url_for("auth.login"))

    # Save token in session
    session["google_token"] = (resp["access_token"], "")

    # Get user info from Google
    user_info = google.get("userinfo")
    session["user_email"] = user_info.data["email"]

    # Remove token from session after use
    session.pop("google_token")

    # Track successful login
    analytics = current_app.config.get("ANALYTICS_SERVICE")
    if analytics:
        analytics.capture_event("login_successful", {"email": session["user_email"]})

    # Get the next URL from session or default to home
    next_url = session.pop("next_url", url_for("main.home"))

    # Redirect to the next URL
    return redirect(next_url)


@bp.route("/logout")
def logout():
    # Track logout
    analytics = current_app.config.get("ANALYTICS_SERVICE")
    if analytics and "user_email" in session:
        analytics.capture_event("logout", {"email": session["user_email"]})

    # Clear session
    session.pop("user_email", None)

    return redirect(url_for("main.home"))
