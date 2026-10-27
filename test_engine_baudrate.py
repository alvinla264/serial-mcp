"""Tests for driver-level baud changes without operation-wide serialization."""

from serial_mcp_engine import SerialState


class FakeDriver:
    def __init__(self, rate=9600, fail=False):
        self._baudrate = rate
        self.fail = fail

    def close(self):
        pass

    @property
    def baudrate(self):
        return self._baudrate

    @baudrate.setter
    def baudrate(self, value):
        if self.fail:
            raise OSError("driver rejected rate")
        self._baudrate = value


class FakeTransport:
    kind = "direct"
    is_open = True
    port = "/dev/pts/test"

    def __init__(self, driver):
        self.ser = driver
        self.baudrate = driver.baudrate

    def close(self):
        self.ser.close()


def test_baud_change_succeeds_without_operation_lock_and_sends_no_io(tmp_path):
    state = SerialState(log_path=str(tmp_path / "engine.log"))
    driver = FakeDriver()
    state.transport = FakeTransport(driver)
    state.operation_lock.acquire()
    try:
        assert state.set_baudrate(19200) == "Baudrate set to 19200."
    finally:
        state.operation_lock.release()
    assert state.transport.baudrate == 19200
    assert driver.baudrate == 19200
    assert not state.io_history
    state.disconnect()


def test_baud_change_failure_preserves_actual_setting(tmp_path):
    state = SerialState(log_path=str(tmp_path / "engine.log"))
    driver = FakeDriver(fail=True)
    state.transport = FakeTransport(driver)
    result = state.set_baudrate(19200)
    assert "actual baudrate is 9600" in result
    assert state.transport.baudrate == driver.baudrate == 9600
    state.disconnect()


def test_baud_change_requires_open_transport_and_valid_integer(tmp_path):
    state = SerialState(log_path=str(tmp_path / "engine.log"))
    assert state.set_baudrate(19200).startswith("Error: Not connected")
    state.transport = FakeTransport(FakeDriver())
    assert state.set_baudrate(True).startswith("Error:")
    state.disconnect()
