from .align import run as run_align
from .classify import run as run_classify
from .detect import run as run_detect
from .embed import run as run_embed
from .extract import run as run_extract
from .infer import run as run_infer
from .normalize import run as run_normalize
from .structure import run as run_structure

__all__ = [
    "run_embed",
    "run_align",
    "run_extract",
    "run_infer",
    "run_normalize",
    "run_detect",
    "run_classify",
    "run_structure",
]
