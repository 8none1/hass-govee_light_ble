import asyncio
from datetime import datetime
import bleak_retry_connector
from bleak.backends.characteristic import BleakGATTCharacteristic
from bleak import (
    BleakClient,
    BLEDevice
)
from .const import WRITE_CHARACTERISTIC_UUID, READ_CHARACTERISTIC_UUID
from .api_utils import (
    LedPacketHead,
    LedPacketCmd,
    LedColorType,
    LedPacket,
    GoveeUtils
)

import logging
_LOGGER = logging.getLogger(__name__)

DISCONNECT_TIMEOUT = 20  # seconds

class GoveeAPI:
    state: bool | None = None
    brightness: int | None = None
    color: tuple[int, ...] | None = None

    def __init__(self, ble_device: BLEDevice, update_callback, segmented: bool = False):
        self._conn = None
        self._ble_device = ble_device
        self._segmented = segmented
        self._packet_buffer = []
        self._client = None
        self._update_callback = update_callback
        self._last_activity: datetime | None = None
        self._disconnect_task: asyncio.Task | None = None
        self._connection_lock = asyncio.Lock()

    @property
    def address(self):
        return self._ble_device.address

    async def _schedule_disconnect(self):
        """Schedule disconnection after timeout period."""
        if self._disconnect_task:
            self._disconnect_task.cancel()
        
        async def _disconnect_after_timeout():
            try:
                await asyncio.sleep(DISCONNECT_TIMEOUT)
                async with self._connection_lock:
                    if self._client and self._client.is_connected:
                        _LOGGER.debug("Disconnecting %s due to inactivity timeout", self.address)
                        try:
                            await self._client.disconnect()
                        except Exception as ex:
                            _LOGGER.debug("Error during timeout disconnect: %s", ex)
                        finally:
                            self._client = None
                            self._last_activity = None
            except asyncio.CancelledError:
                pass
        
        self._disconnect_task = asyncio.create_task(_disconnect_after_timeout())

    async def _ensureConnected(self):
        """ connects to a bluetooth device """
        async with self._connection_lock:
            if self._client is not None and self._client.is_connected:
                self._last_activity = datetime.now()
                return None
            await self._connect()
            self._last_activity = datetime.now()
    
    async def _connect(self):
        self._client = await bleak_retry_connector.establish_connection(BleakClient, self._ble_device, self.address)
        await self._client.start_notify(READ_CHARACTERISTIC_UUID, self._handleReceive)

    async def _transmitPacket(self, packet: LedPacket):
        """ transmit the actiual packet """
        #convert to bytes
        frame = await GoveeUtils.generateFrame(packet)
        #transmit to UUID
        await self._client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, frame, False)
        self._last_activity = datetime.now()

    async def _handleRequest(self, packet: LedPacket):
        """ process received responses """
        match packet.cmd:
            case LedPacketCmd.POWER:
                self.state = packet.payload[0] == 0x01
            case LedPacketCmd.BRIGHTNESS:
                #segmented devices 0-100
                self.brightness = packet.payload[0] / 100 * 255 if self._segmented else packet.payload[0]
            case LedPacketCmd.COLOR:
                red = packet.payload[1]
                green = packet.payload[2]
                blue = packet.payload[3]
                self.color = (red, green, blue)
            case LedPacketCmd.SEGMENT:
                red = packet.payload[2]
                green = packet.payload[3]
                blue = packet.payload[4]
                self.color = (red, green, blue)

    async def _handleReceive(self, characteristic: BleakGATTCharacteristic, frame: bytearray):
        """ receives packets async """
        if not await GoveeUtils.verifyChecksum(frame):
            raise Exception("transmission error, received packet with bad checksum")
        
        packet = LedPacket(
            head=frame[0],
            cmd=frame[1],
            payload=frame[2:-1]
        )
        #only requests are expected to send a response
        if packet.head == LedPacketHead.REQUEST:
            await self._handleRequest(packet)
            await self._update_callback()

    async def _preparePacket(self, cmd: LedPacketCmd, payload: bytes | list = b'', request: bool = False, repeat: int = 3):
        """ add data to transmission buffer """
        #request data or perform a change
        head = LedPacketHead.REQUEST if request else LedPacketHead.COMMAND
        packet = LedPacket(head, cmd, payload)
        for index in range(repeat):
            self._packet_buffer.append(packet)

    async def _clearPacketBuffer(self):
        """ clears the packet buffer """
        self._packet_buffer = []

    async def sendPacketBuffer(self):
        """ transmits all buffered data """
        if not self._packet_buffer:
            #nothing to do
            return None
        await self._ensureConnected()
        for packet in self._packet_buffer:
            await self._transmitPacket(packet)
        await self._clearPacketBuffer()
        #schedule disconnect after inactivity
        await self._schedule_disconnect()

    async def requestStateBuffered(self):
        """ adds a request for the current power state to the transmit buffer """
        await self._preparePacket(LedPacketCmd.POWER, request=True)

    async def requestBrightnessBuffered(self):
        """ adds a request for the current brightness state to the transmit buffer """
        await self._preparePacket(LedPacketCmd.BRIGHTNESS, request=True)

    async def requestColorBuffered(self):
        """ adds a request for the current color state to the transmit buffer """
        if self._segmented:
            #0x01 means first segment
            await self._preparePacket(LedPacketCmd.SEGMENT, b'\x01', request=True)
        else:
            #legacy devices
            await self._preparePacket(LedPacketCmd.COLOR, request=True)
    
    async def setStateBuffered(self, state: bool):
        """ adds the state to the transmit buffer """
        if self.state == state:
            return None #nothing to do
        #0x1 = ON, Ox0 = OFF
        await self._preparePacket(LedPacketCmd.POWER, [0x1 if state else 0x0])
        await self.requestStateBuffered()
    
    async def setBrightnessBuffered(self, brightness: int):
        """ adds the brightness to the transmit buffer """
        if self.brightness == brightness:
            return None #nothing to do
        #legacy devices 0-255
        payload = round(brightness)
        if self._segmented:
            #segmented devices 0-100
            payload = round(brightness / 255 * 100)
        await self._preparePacket(LedPacketCmd.BRIGHTNESS, [payload])
        await self.requestBrightnessBuffered()
        
    async def setColorBuffered(self, red: int, green: int, blue: int):
        """ adds the color to the transmit buffer """
        if self.color == (red, green, blue):
            return None #nothing to do
        if self._segmented:
            await self._preparePacket(LedPacketCmd.COLOR, [LedColorType.SEGMENTS, 0x01, red, green, blue, 0, 0, 0, 0, 0, 0xff, 0xff])
        else:
            #legacy devices
            await self._preparePacket(LedPacketCmd.COLOR, [LedColorType.SINGLE, red, green, blue])
            await self._preparePacket(LedPacketCmd.COLOR, [LedColorType.LEGACY, red, green, blue])
        await self.requestColorBuffered()

    async def disconnect(self):
        """Manually disconnect from the device."""
        if self._disconnect_task:
            self._disconnect_task.cancel()
            self._disconnect_task = None
        
        async with self._connection_lock:
            if self._client and self._client.is_connected:
                try:
                    await self._client.disconnect()
                    _LOGGER.debug("Manually disconnected from %s", self.address)
                except Exception as ex:
                    _LOGGER.error("Error disconnecting from %s: %s", self.address, ex)
                finally:
                    self._client = None
                    self._last_activity = None
