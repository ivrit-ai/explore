from urllib.parse import urlencode
from starlette.requests import Request
from starlette.responses import HTMLResponse


def _nested_routes(route):
    """The routes contained by `route`, whatever shape the router uses.

    Starlette <1.0 flattened included routers into app.routes. From 1.0 an
    included router stays a single entry that holds its own routes, so a flat
    scan finds nothing and every url_for lookup fails.
    """
    nested = getattr(route, "routes", None)
    if nested is not None:
        return nested
    inner = getattr(route, "original_router", None)
    return getattr(inner, "routes", None)


def find_route(routes, name: str):
    """Depth-first search for a route by its declared name."""
    for route in routes:
        if getattr(route, "name", None) == name:
            return route
        nested = _nested_routes(route)
        if nested:
            found = find_route(nested, name)
            if found is not None:
                return found
    return None


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
        route = find_route(request.app.routes, name)

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
