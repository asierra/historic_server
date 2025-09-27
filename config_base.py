from abc import ABC, abstractmethod
from typing import List, ClassVar

class SatelliteConfigBase(ABC):
    """
    Abstract Base Class that defines the contract for any satellite configuration.
    All satellite-specific configuration classes must inherit from this class
    and implement all its abstract methods and properties.
    """

    # --- Abstract Class-level Properties ---
    # These must be defined in the concrete subclass.
    
    @property
    @abstractmethod
    def VALID_SATELLITES(self) -> List[str]: ...
    
    @property
    @abstractmethod
    def DEFAULT_SATELLITE(self) -> str: ...

    @property
    @abstractmethod
    def VALID_LEVELS(self) -> List[str]: ...

    @property
    @abstractmethod
    def DEFAULT_LEVEL(self) -> str: ...

    @property
    @abstractmethod
    def VALID_DOMAINS(self) -> List[str]: ...

    @property
    @abstractmethod
    def VALID_PRODUCTS(self) -> List[str]: ...

    @property
    @abstractmethod
    def VALID_BANDAS(self) -> List[str]: ...

    @property
    @abstractmethod
    def DEFAULT_BANDAS(self) -> List[str]: ...