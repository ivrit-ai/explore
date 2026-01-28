import logging
import time
from functools import wraps

import posthog
from flask import current_app, request, session

logger = logging.getLogger(__name__)


class AnalyticsService:
    def __init__(self, api_key, host="https://app.posthog.com", disabled=False):
        self.api_key = api_key
        self.host = host
        self.disabled = disabled

        if not disabled:
            try:
                posthog.api_key = api_key
                posthog.host = host
                logger.info(f"PostHog analytics initialized with host: {host}")
            except Exception as e:
                logger.error(f"Failed to initialize PostHog: {str(e)}")
                self.disabled = True

    def identify_user(self, user_id, properties=None):
        """Identify a user with optional properties"""
        if self.disabled:
            return

        try:
            posthog.identify(user_id, properties or {})
            logger.debug(f"Identified user: {user_id}")
        except Exception as e:
            logger.error(f"Failed to identify user: {str(e)}")

    def capture_event(self, event_name, properties=None, user_id=None):
        """Capture an event with properties"""
        if self.disabled:
            return

        # Initialize properties if None
        properties = properties or {}
        properties["source"] = "explore.ivrit.ai"

        # Add user email to properties if available in session
        if "user_email" in session:
            properties["user_email"] = session["user_email"]

        try:
            posthog.capture(user_id or self._get_user_id(), event_name, properties)
            logger.debug(f"Captured event: {event_name}")
        except Exception as e:
            logger.error(f"Failed to capture event: {str(e)}")

    def capture_search(
        self,
        query,
        use_substring=False,
        max_results_per_page=None,
        page=1,
        execution_time_ms=None,
        results_count=0,
        total_results=0,
        progressive=False,
    ):
        """Track search events with detailed properties"""
        if self.disabled:
            return

        try:
            properties = {
                "query": query,
                "use_substring": use_substring,
                "max_results_per_page": max_results_per_page,
                "page": page,
                "execution_time_ms": execution_time_ms,
                "results_count": results_count,
                "total_results": total_results,
                "progressive": progressive,
            }

            # Add user email to properties if available
            if "user_email" in session:
                properties["user_email"] = session["user_email"]

            self.capture_event("search_executed", properties)
            logger.debug(f"Tracked search: {query}")
        except Exception as e:
            logger.error(f"Failed to track search: {str(e)}")

    def capture_export(self, export_type, query=None, source=None, format=None, execution_time_ms=None):
        """Capture export event with details"""
        properties = {
            "export_type": export_type,
            "execution_time_ms": execution_time_ms,
            "url": request.url,
        }

        if query:
            properties["query"] = query
        if source:
            properties["source"] = source
        if format:
            properties["format"] = format

        # Add user email to properties if available
        if "user_email" in session:
            properties["user_email"] = session["user_email"]

        self.capture_event("content_exported", properties)

    def capture_error(self, error_type, error_message, context=None):
        """Capture error events with context"""
        properties = {
            "error_type": error_type,
            "error_message": error_message,
            "url": request.url,
            "method": request.method,
            "user_agent": request.user_agent.string,
        }

        if context:
            properties.update(context)

        # Add user email to properties if available
        if "user_email" in session:
            properties["user_email"] = session["user_email"]

        self.capture_event("error_occurred", properties)

    def _get_user_id(self):
        """Get user ID from session or generate anonymous ID"""
        # Use email as user ID if available, otherwise use IP
        if "user_email" in session:
            return session["user_email"]
        return request.remote_addr


# Create a timing decorator for performance tracking
def track_performance(event_name, include_args=None):
    """Decorator to track performance of functions"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()

            try:
                result = func(*args, **kwargs)
                success = True
            except Exception as e:
                success = False
                error = str(e)
                raise
            finally:
                execution_time = (time.time() - start_time) * 1000  # ms

                # Get analytics service from app context
                analytics = current_app.config.get("ANALYTICS_SERVICE")
                if analytics and not analytics.disabled:
                    properties = {"execution_time_ms": execution_time, "success": success}

                    # Include specified arguments in properties
                    if include_args:
                        for arg_name in include_args:
                            if arg_name in kwargs:
                                properties[arg_name] = kwargs[arg_name]

                    if not success:
                        properties["error"] = error

                    # Add user email to properties if available
                    if "user_email" in session:
                        properties["user_email"] = session["user_email"]

                    analytics.capture_event(event_name, properties)

            return result

        return wrapper

    return decorator
