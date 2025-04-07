from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class IdpUser:
    id: str
    email: str


class IdentityProvider(ABC):
    @abstractmethod
    def get_users(self) -> list[IdpUser]:
        pass
