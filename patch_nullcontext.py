# patch_nullcontext.py
import sys
import mode.utils.contexts

if not hasattr(mode.utils.contexts, 'nullcontext'):
    from contextlib import nullcontext as _nullcontext
    mode.utils.contexts.nullcontext = _nullcontext
    print("✅ nullcontext successfully patched into mode.utils.contexts")