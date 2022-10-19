
from abc import ABC, abstractmethod
from bokeh.palettes import Spectral6, Category10
from bokeh.models import  CategoricalColorMapper
from typing import List, Dict

class BaseComponent(ABC):
    def __init__(self) -> None:
        super().__init__()
    
    @abstractmethod
    def get_layouts(self):
        raise NotImplementedError('Please implement methods')

    def _get_categorical_palette(self, factors: List[str]) -> Dict[str, str]:
        n = max(3, len(factors))
        palette = Category10
        if n < len(palette):
            _palette = palette[n%10]
        elif n < 21:
            from bokeh.palettes import Category20
            _palette = Category20[n]
        else:
            from bokeh.palettes import viridis
            _palette = viridis(n)
        return CategoricalColorMapper(factors=factors, palette=_palette)