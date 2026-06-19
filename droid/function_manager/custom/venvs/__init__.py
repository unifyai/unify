"""
Custom virtual environments.

Per-client venvs are defined under ``droid/customization/clients/<client>/venvs/``
as ``.toml`` files containing pyproject.toml content.  The filename (without .toml)
becomes the venv name for reference in ``@custom_function(venv_name="...")``.
"""
