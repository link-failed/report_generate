
from abc import ABC, abstractmethod


class BaseComponent(ABC):
    def __init__(self) -> None:
        super().__init__()
    
    @abstractmethod
    def get_layouts(self):
        raise NotImplementedError('Please implement methods')