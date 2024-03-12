from pydantic import BaseModel


class Sensor(BaseModel):
    id: int
    name: str
    latitude: float
    longitude: float
    joined_at: str
    type: str
    mac_address: str

    class Config:
        orm_mode = True


class SensorTemperature(Sensor):
    last_seen: str
    battery_level: float
    temperature: float
    humidity: float


class SensorVelocity(Sensor):
    last_seen: str
    battery_level: float
    velocity: float


class SensorCreate(BaseModel):
    name: str
    longitude: float
    latitude: float
    type: str
    mac_address: str
    manufacturer: str
    model: str
    serie_number: str
    firmware_version: str


class SensorData(BaseModel):
    battery_level: float
    last_seen: str


class SensorDataTemperature(SensorData):
    temperature: float
    humidity: float


class SensorDataVelocity(SensorData):
    velocity: float
