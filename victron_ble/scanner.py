import inspect
import json
import logging
import time
from enum import Enum
from typing import Set
from box import Box
import yaml
import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData

from victron_ble.devices import Device, DeviceData, detect_device_type
from victron_ble.exceptions import AdvertisementKeyMissingError, UnknownDeviceError

logger = logging.getLogger(__name__)

with open("mqtt_config.yml", "r") as yamlfile:
    config = Box(yaml.load(yamlfile, Loader=yaml.FullLoader))
    print("config read successful")



class BaseScanner:
    def __init__(self) -> None:
        """Initialize the scanner."""
        self._scanner: BleakScanner = BleakScanner(
            detection_callback=self._detection_callback
        )
        self._seen_data: Set[bytes] = set()

    def _detection_callback(self, device: BLEDevice, advertisement: AdvertisementData):
        # Filter for Victron devices and instant readout advertisements
        data = advertisement.manufacturer_data.get(0x02E1)
        if not data or not data.startswith(b"\x10") or data in self._seen_data:
            return

        # De-duplicate advertisements
        if len(self._seen_data) > 1000:
            self._seen_data = set()
        self._seen_data.add(data)

        self.callback(device, data)

    def callback(self, device: BLEDevice, data: bytes):
        raise NotImplementedError()

    async def start(self):
        await self._scanner.start()

    async def stop(self):
        await self._scanner.stop()


# An ugly hack to print a class as JSON
class DeviceDataEncoder(json.JSONEncoder):
    def default(self, obj):
        if issubclass(obj.__class__, DeviceData):
            data = {}
            for name, method in inspect.getmembers(obj, predicate=inspect.ismethod):
                if name.startswith("get_"):
                    value = method()
                    if isinstance(value, Enum):
                        value = value.name.lower()
                    if value is not None:
                        data[name[4:]] = value
            return data


class Scanner(BaseScanner):
    def __init__(self, device_keys: dict[str, str] = {}):
        super().__init__()
        self._device_keys = {k.lower(): v for k, v in device_keys.items()}
        self._known_devices: dict[str, Device] = {}

    async def start(self):
        logger.info(f"Reading data for {list(self._device_keys.keys())}")
        await super().start()

    def get_device(self, ble_device: BLEDevice, raw_data: bytes) -> Device:
        address = ble_device.address.lower()
        if address not in self._known_devices:
            advertisement_key = self.load_key(address)

            device_klass = detect_device_type(raw_data)
            if not device_klass:
                raise UnknownDeviceError(
                    f"Could not identify device type for {ble_device}"
                )

            self._known_devices[address] = device_klass(advertisement_key)
        return self._known_devices[address]

    def load_key(self, address: str) -> str:
        try:
            return self._device_keys[address]
        except KeyError:
            raise AdvertisementKeyMissingError(f"No key available for {address}")

    def callback(self, ble_device: BLEDevice, raw_data: bytes):
        logger.debug(
            f"Received data from {ble_device.address.lower()}: {raw_data.hex()}"
        )
        try:
            device = self.get_device(ble_device, raw_data)
        except AdvertisementKeyMissingError:
            return
        except UnknownDeviceError as e:
            logger.error(e)
            return
        parsed = device.parse(raw_data)

        blob = {
            "name": ble_device.name,
            "address": ble_device.address,
            "rssi": ble_device.rssi,
            "payload": parsed,
        }
        print(json.dumps(blob, cls=DeviceDataEncoder, indent=2))

        client = mqtt.Client()
        client.username_pw_set(config.user, config.pw)
        client.connect(config.hostname)

        m = "bo_lifepo4_voltage"
        mqtt_topic = config.out_topic + m
        mqtt_payload = m + " volt=" + str(parsed.get_voltage())

        client.publish(mqtt_topic, mqtt_payload)

        m = "bo_starter_voltage"
        mqtt_topic = config.out_topic + m
        mqtt_payload = m + " volt=" + str(parsed.get_starter_voltage())

        client.publish(mqtt_topic, mqtt_payload)

        m = "bo_current"
        mqtt_topic = config.out_topic + m
        mqtt_payload = m + " ampere=" + str(parsed.get_current())

        client.publish(mqtt_topic, mqtt_payload)

        m = "bo_consumed_ah"
        mqtt_topic = config.out_topic + m
        mqtt_payload = m + " ah=" + str(parsed.get_consumed_ah())

        client.publish(mqtt_topic, mqtt_payload)

        m = "bo_emaining_mins"
        mqtt_topic = config.out_topic + m
        mqtt_payload = m + " mins=" + str(parsed.get_remaining_mins())
        client.publish(mqtt_topic, mqtt_payload)

        m = "bo_soc"
        mqtt_topic = config.out_topic + m
        mqtt_payload = m + " soc=" + str(parsed.get_soc())
        client.publish(mqtt_topic, mqtt_payload)

        m = "bo_power"
        mqtt_topic = config.out_topic + m
        mqtt_payload = m + " watt=" + str(parsed.get_current() * parsed.get_voltage() )
        client.publish(mqtt_topic, mqtt_payload)

        client.disconnect()


class DiscoveryScanner(BaseScanner):
    def __init__(self) -> None:
        super().__init__()
        self._seen_devices: Set[str] = set()

    def callback(self, device: BLEDevice, advertisement: bytes):
        if device.address not in self._seen_devices:
            logger.info(f"{device}")
            self._seen_devices.add(device.address)


class DebugScanner(BaseScanner):
    def __init__(self, address: str):
        super().__init__()
        self.address = address

    async def start(self):
        logger.info(f"Dumping advertisements from {self.address}")
        await super().start()

    def callback(self, device: BLEDevice, data: bytes):
        if device.address.lower() == self.address.lower():
            logger.info(f"{time.time():<24}: {data.hex()}")
