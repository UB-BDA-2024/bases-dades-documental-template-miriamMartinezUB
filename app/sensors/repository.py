import json
from typing import List, Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.redis_client import RedisClient
from . import models, schemas
from ..mongodb_client import MongoDBClient

_SENSOR_COLLECTION = 'sensors'


def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()


def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()


def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()


def create_sensor(mongo_client: MongoDBClient, db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    # Create in mySQL
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    # Create in MongoDb
    collection = mongo_client.getCollection(_SENSOR_COLLECTION)
    collection.insert_one(sensor.dict())
    return db_sensor


def record_data(redis: RedisClient, mongo_client: MongoDBClient, db: Session, sensor_id: int,
                data: schemas.SensorData) -> schemas.Sensor:
    redis.set(sensor_id, data.json())
    sensor = _from_id_and_data_to_sensor(
        sensor=_get_sensor_from_sensor_id(mongo_client=mongo_client, db=db, sensor_id=sensor_id),
        data=data)
    return sensor


def get_data(redis: RedisClient, mongo_client: MongoDBClient, db: Session, sensor_id: int) -> schemas.Sensor:
    sensor = _get_sensor_from_sensor_id(db=db, mongo_client=mongo_client, sensor_id=sensor_id)
    sensor_with_data = _from_id_and_data_to_sensor(sensor=sensor,
                                                   data=_get_data(redis=redis, sensor_id=sensor_id, type=sensor.type))
    return sensor_with_data


def delete_sensor(db: Session, redis: RedisClient, mongo_client: MongoDBClient, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    # Delete from mongo
    collection = mongo_client.getCollection(_SENSOR_COLLECTION)
    collection.delete_one({"name": db_sensor.name})
    # Delete from redis
    redis.delete(sensor_id)
    # Delete from SQL
    db.delete(db_sensor)
    db.commit()
    return db_sensor


def get_sensors_near(db: Session, redis: RedisClient, mongo_client: MongoDBClient, latitude: float, longitude: float,
                     radius: float) -> \
        List[schemas.Sensor]:
    sensors = []

    collection = mongo_client.getCollection(_SENSOR_COLLECTION)
    radius_in_degrees = radius / 111.12
    sensors_dicts = collection.find({
        'latitude': {'$gte': latitude - radius_in_degrees, '$lte': latitude + radius_in_degrees},
        'longitude': {'$gte': longitude - radius_in_degrees, '$lte': longitude + radius_in_degrees}
    })
    for sensor_dict in sensors_dicts:
        sensor_create = schemas.SensorCreate(**sensor_dict)
        db_sensor = db.query(models.Sensor).filter(models.Sensor.name == sensor_create.name).first()
        sensor = _get_sensor_from_db_sensor_and_sensor_create(db_sensor=db_sensor, sensor_create=sensor_create)
        sensor_data = _get_data(redis=redis, sensor_id=db_sensor.id, type=sensor.type)
        if sensor_data is None:
            sensors.append(sensor)
        else:
            sensor_with_data = _from_id_and_data_to_sensor(sensor=sensor, data=sensor_data)
            sensors.append(sensor_with_data)
    return sensors


def _get_data(redis: RedisClient, sensor_id: int, type: str) -> schemas.SensorData | None:
    redis_data = redis.get(sensor_id)
    if redis_data is None:
        raise HTTPException(status_code=404, detail="Data not found")
    # Parse json to dict
    data_dict = json.loads(redis_data)
    # get Sensor Data from dict
    match type:
        case 'Temperatura':
            sensor_data = schemas.SensorDataTemperature(**data_dict)
        case 'Velocitat':
            sensor_data = schemas.SensorDataVelocity(**data_dict)
        case _:
            raise HTTPException(status_code=409, detail="Conflict - This type of sensor doesn't exist")
    return sensor_data


def _from_id_and_data_to_sensor(sensor: schemas.Sensor, data: schemas.SensorData) -> schemas.Sensor:
    match sensor.type:
        case 'Temperatura':
            if type(data) is not schemas.SensorDataTemperature:
                raise HTTPException(status_code=409,
                                    detail="Conflict - The sensor with the specific id is of type velocity and you give data of temperature sensor")
            return schemas.SensorTemperature(id=sensor.id, name=sensor.name, latitude=sensor.latitude,
                                             longitude=sensor.longitude, joined_at=sensor.joined_at, type=sensor.type,
                                             mac_address=sensor.name, last_seen=data.last_seen,
                                             battery_level=data.battery_level, temperature=data.temperature,
                                             humidity=data.humidity)
        case 'Velocitat':
            if type(data) is not schemas.SensorDataVelocity:
                raise HTTPException(status_code=409,
                                    detail="Conflict - The sensor with the specific id is of type temperature and you give data of velocity sensor")
            return schemas.SensorVelocity(id=sensor.id, name=sensor.name, latitude=sensor.latitude,
                                          longitude=sensor.longitude, joined_at=sensor.joined_at, type=sensor.type,
                                          mac_address=sensor.name, last_seen=data.last_seen,
                                          battery_level=data.battery_level, velocity=data.velocity)
        case _:
            raise HTTPException(status_code=409, detail="Conflict - This type of sensor doesn't exist")


def _get_sensor_from_sensor_id(db: Session, mongo_client: MongoDBClient, sensor_id: int) -> schemas.Sensor:
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    collection = mongo_client.getCollection(_SENSOR_COLLECTION)
    sensor_dict = collection.find_one({"name": db_sensor.name})
    sensor_create = schemas.SensorCreate(**sensor_dict)
    return _get_sensor_from_db_sensor_and_sensor_create(db_sensor=db_sensor, sensor_create=sensor_create)


def _get_sensor_from_db_sensor_and_sensor_create(db_sensor: models.Sensor,
                                                 sensor_create: schemas.SensorCreate) -> models.Sensor:
    return schemas.Sensor(id=db_sensor.id, name=sensor_create.name,
                          latitude=sensor_create.latitude,
                          longitude=sensor_create.longitude,
                          joined_at=str(db_sensor.joined_at),
                          type=sensor_create.type,
                          mac_address=sensor_create.name)
