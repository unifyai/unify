"""Verification of compositional functions: effect classes, trust hash, policy.

``classify`` derives a lower-bound effect class from the source, ``ledger``
computes the trust hash and folds verdict rows, ``policy`` derives
``Function.verify`` from the fold. Nothing in this package calls a model.
"""
