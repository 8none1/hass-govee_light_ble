from dataclasses import dataclass
from datetime import timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_ADDRESS, CONF_NAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.components import bluetooth

from .const import DOMAIN
from .api import GoveeAPI

import logging
_LOGGER = logging.getLogger(__name__)

@dataclass
class GoveeApiData:
    """Class to hold api data."""

    state: bool | None = None
    brightness: int | None = None
    color: tuple[int, ...] | None = None

class GoveeCoordinator(DataUpdateCoordinator):
    """Coordinator to manage Govee device updates."""

    data: GoveeApiData

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry) -> None:
        """Initialize coordinator."""

        # Set variables from values entered in config flow setup
        self.device_name = config_entry.data[CONF_NAME]
        self.device_address = config_entry.data[CONF_ADDRESS]
        self.device_segmented = config_entry.data["segmented"]

        #get connection to bluetooth device
        ble_device = bluetooth.async_ble_device_from_address(
            hass,
            self.device_address,
            connectable=False
        )
        assert ble_device
        self._api = GoveeAPI(ble_device, self._async_push_data, self.device_segmented)

        # Initialise DataUpdateCoordinator
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN} ({config_entry.unique_id})",
            # Disable automatic polling by setting a very long interval
            # The device will only be queried when commands are sent
            update_interval=None,
        )

    def _get_data(self):
        """Get the current data from the API."""
        return GoveeApiData(
            state=self._api.state,
            brightness=self._api.brightness,
            color=self._api.color
        )

    async def _async_push_data(self):
        self.async_set_updated_data(self._get_data())

    async def _async_update_data(self):
        """Fetch data from API endpoint."""
        # No automatic polling - state will be updated after commands
        # or can be manually refreshed if needed
        return self._get_data()

    async def async_refresh_state(self):
        """Manually refresh the device state when needed."""
        try:
            await self._api.requestStateBuffered()
            await self._api.requestBrightnessBuffered()
            await self._api.requestColorBuffered()
            await self._api.sendPacketBuffer()
            await self.async_refresh()
        except Exception as err:
            _LOGGER.error("Error refreshing state: %s", err)

    async def setStateBuffered(self, state: bool):
        await self._api.setStateBuffered(state)

    async def setBrightnessBuffered(self, brightness: int):
        await self._api.setBrightnessBuffered(brightness)

    async def setColorBuffered(self, red: int, green: int, blue: int):
        await self._api.setColorBuffered(red, green, blue)

    async def sendPacketBuffer(self):
        await self._api.sendPacketBuffer()
