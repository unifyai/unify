"""
Keep the hang fixture out of directory sweeps.

``test_hang.py`` sleeps for an hour by design: it exists so the
``--session-timeout`` test can watch parallel_run.sh kill a hung session.
``collect_ignore`` hides it from ``pytest tests/parallel_run`` and similar
sweeps, while a path named explicitly on the command line (which is how
that test hands it to the runner) is still collected.
"""

collect_ignore = ["test_hang.py"]
