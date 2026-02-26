import posthog
import logging

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
            posthog.identify(
                user_id,
                properties or {}
            )
            logger.debug(f"Identified user: {user_id}")
        except Exception as e:
            logger.error(f"Failed to identify user: {str(e)}")

    def capture_event(self, event_name, properties=None, user_id=None, user_email=None):
        """Capture an event with properties"""
        if self.disabled:
            return

        properties = properties or {}
        properties['source'] = 'explore.ivrit.ai'

        if user_email:
            properties['user_email'] = user_email

        try:
            posthog.capture(
                user_id or self._get_user_id(user_email=user_email),
                event_name,
                properties
            )
            logger.debug(f"Captured event: {event_name}")
        except Exception as e:
            logger.error(f"Failed to capture event: {str(e)}")

    def capture_search(self, query, use_substring=False, max_results_per_page=None,
                      page=1, execution_time_ms=None, results_count=0, total_results=0,
                      progressive=False, user_email=None):
        """Track search events with detailed properties"""
        if self.disabled:
            return

        try:
            properties = {
                'query': query,
                'use_substring': use_substring,
                'max_results_per_page': max_results_per_page,
                'page': page,
                'execution_time_ms': execution_time_ms,
                'results_count': results_count,
                'total_results': total_results,
                'progressive': progressive
            }

            if user_email:
                properties['user_email'] = user_email

            self.capture_event('search_executed', properties, user_email=user_email)
            logger.debug(f"Tracked search: {query}")
        except Exception as e:
            logger.error(f"Failed to track search: {str(e)}")

    def capture_export(self, export_type, query=None, source=None, format=None,
                      execution_time_ms=None, url=None, user_email=None):
        """Capture export event with details"""
        properties = {
            'export_type': export_type,
            'execution_time_ms': execution_time_ms,
        }

        if url:
            properties['url'] = url
        if query:
            properties['query'] = query
        if source:
            properties['source'] = source
        if format:
            properties['format'] = format
        if user_email:
            properties['user_email'] = user_email

        self.capture_event('content_exported', properties, user_email=user_email)

    def capture_error(self, error_type, error_message, context=None,
                     url=None, method=None, user_agent=None, user_email=None):
        """Capture error events with context"""
        properties = {
            'error_type': error_type,
            'error_message': error_message,
        }

        if url:
            properties['url'] = url
        if method:
            properties['method'] = method
        if user_agent:
            properties['user_agent'] = user_agent
        if context:
            properties.update(context)
        if user_email:
            properties['user_email'] = user_email

        self.capture_event('error_occurred', properties, user_email=user_email)

    def _get_user_id(self, user_email=None, remote_addr=None):
        """Get user ID from explicit params or return anonymous"""
        if user_email:
            return user_email
        if remote_addr:
            return remote_addr
        return 'anonymous'
