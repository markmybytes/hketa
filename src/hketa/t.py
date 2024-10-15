from typing import Literal, Optional, TypedDict


Transport = Literal['ctb', 'kmb', 'lrt', 'lrtfeeder', 'nlb', 'mtr']

Language = Literal['zh', 'en']


class Eta(TypedDict):
    class Extras(TypedDict):
        destinaion: Optional[str]
        varient: Optional[str]
        platform: Optional[str]
        car_length: Optional[int]

    eta: str
    is_arriving: bool
    is_scheduled: bool
    extras: Extras
    remark: Optional[str]


class Etas(TypedDict):
    timestamp: str
    message: Optional[str]
    etas: Optional[Eta]


class Route(TypedDict):
    class Service(TypedDict):
        id: str
        gtfs_id: Optional[str]
        description: Optional[str]
        orig: dict[Language, str]
        dest: dict[Language, str]

    outbound: list[Service]
    inbound: list[Service]


class Stop(TypedDict):
    id: str
    seq: int
    name: dict[Language, str]
