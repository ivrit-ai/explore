from urllib.parse import urlencode
from starlette.requests import Request
from starlette.responses import HTMLResponse


def render(request: Request, template_name: str, **context):
    """Render a Jinja2 template with a Flask-compatible url_for injected."""
    templates = request.app.state.templates

    def url_for(name: str, **params):
        # Static files: translate Flask's filename= to Starlette's path=
        if name == 'static':
            path = params.pop('filename', params.pop('path', ''))
            return request.url_for('static', path=path)

        # For named routes, separate path params from query params.
        # Starlette's url_for only accepts path params; extras become query string.
        route = None
        for r in request.app.routes:
            if getattr(r, 'name', None) == name:
                route = r
                break

        if route is None:
            raise ValueError(f"No route named '{name}'")

        # Determine which params are path parameters
        path_param_names = set()
        if hasattr(route, 'param_convertors'):
            path_param_names = set(route.param_convertors.keys())
        elif hasattr(route, 'path'):
            # Parse path params from the route path pattern
            import re
            path_param_names = set(re.findall(r'\{(\w+)', route.path))

        path_params = {}
        query_params = {}
        for k, v in params.items():
            if v is None:
                continue
            if k in path_param_names:
                path_params[k] = v
            else:
                query_params[k] = v

        url = str(request.url_for(name, **path_params))

        if query_params:
            qs = urlencode(query_params)
            url = f"{url}?{qs}"

        return url

    return templates.TemplateResponse(
        request,
        template_name,
        {"url_for": url_for, **context},
    )
