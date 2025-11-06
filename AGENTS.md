# OmniPath Server Contributor Notes

## Project overview
- The server exposes OmniPath's web services through a Sanic ASGI application.
- Database access relies on SQLAlchemy sessions backed by PostgreSQL connections that can be configured through YAML or environment driven parameters (see `omnipath_server/_connection.py`).
- The service layer in `omnipath_server/service/` encapsulates the database aware logic. HTTP handlers in `omnipath_server/server/` should remain thin coordinators that translate between Sanic requests/responses and the service layer to keep compatibility between the two consistent.
- SQLAlchemy table definitions live under `omnipath_server/schema/` and are shared by both the service and loader components.

## Repository layout quick reference
- `omnipath_server/server/`: Sanic blueprints, request parsing, and response serialization.
- `omnipath_server/service/`: orchestrates business rules and mediates between web endpoints and the data layer.
- `omnipath_server/schema/`: SQLAlchemy ORM models and metadata helpers.
- `omnipath_server/_connection.py` & `_session.py`: connection/session helpers for PostgreSQL and SQLAlchemy integration.
- `omnipath_server/loader/`: routines for populating or syncing data between OmniPath and the database.
- `docs/`: Sphinx documentation sources.
- `tests/`: pytest based suite covering service and server behavior.

## Coding guidelines
- Target Python 3.9 and follow the formatting conventions declared in `pyproject.toml` (Black with an 80 character limit and compatible isort settings). The CI enforces flake8, black, and isort, so run them locally before pushing.
- Prefer explicit type hints and descriptive docstrings; align exception handling with the service layer's abstractions (avoid catching broad exceptions in HTTP handlers unless they re-raise service specific errors).
- Keep new Sanic handlers minimal: perform request validation, delegate to the appropriate service function, and translate results into JSON responses. Shared validation utilities belong in `omnipath_server/server` helpers rather than duplicating logic.
- For database interactions, create or reuse service layer helpers so that SQLAlchemy sessions are obtained through `_session.get_session()` (or the relevant helper) rather than instantiating engines manually inside endpoints.
- When adjusting schema models, also check whether loader logic or service serializers require updates to remain in sync.

## Testing and tooling
- Install dependencies with `poetry install`.
- Run unit tests via `poetry run pytest`.
- Use `poetry run tox` for the full matrix mirrored by CI when touching environments or packaging.
- If changes depend on database structure, provide fixtures or mocks so the test suite remains self-contained.

## Documentation and compatibility notes
- Update Sphinx docs in `docs/` when modifying externally visible endpoints or service behaviors.
- When introducing breaking changes to service interfaces, adjust the corresponding server layer adapters and tests in tandem to maintain compatibility between the Sanic handlers and service classes.

## Legacy service cleanup focus
- Make `LegacyServer` pass normalized argument values into `LegacyService`: convert single-item arrays into scalars, split delimited strings, and align booleans/ints with what the service expects to avoid failing queries.
- Exercise the combinatorial request generator in `scripts/r-legacy-server-tests.R`, but trim it when query counts explode; fall back to a curated YAML of representative argument combinations when that delivers better coverage.
- Ensure the R harness covers tricky endpoints and options such as `annotated_network`, `curated_ligand_receptor_interactions`, `extra_attrs`, and evidence toggles; add a switch so expensive, full-database scenarios can be disabled by default.
- Treat initial test success as “no exceptions from the service,” then extend checks to validate returned data frames (mandatory columns, types, and targeted value assertions).
- Reset OmnipathR state between runs (`OmnipathR:::.optrace()` for trace logs, `omnipath_set_cachedir("/tmp/omnipath001")` for a clean cache) so cached responses do not mask regressions.
