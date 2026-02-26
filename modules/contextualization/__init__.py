"""
Contextualization Module

This package contains contextualization engines for key extraction and aliasing.
"""

# Re-export key components from key_extraction_aliasing for convenience
try:
    from .key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
        AliasingEngine,
        AliasingResult,
        AliasRule,
        TransformationType,
    )
    from .key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
        ExtractionResult,
        KeyExtractionEngine,
    )

    __all__ = [
        "KeyExtractionEngine",
        "ExtractionResult",
        "AliasingEngine",
        "AliasingResult",
        "AliasRule",
        "TransformationType",
    ]
except ImportError:
    __all__ = []
