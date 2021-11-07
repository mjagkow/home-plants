import io
import csv
import json
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from struct import unpack

from serial.tools.list_ports_common import ListPortInfo
from serial.tools.list_ports import comports
from serial import Serial
from pydantic import BaseModel, Field


def suggest_comport() -> Optional[ListPortInfo]:
    for port in comports():
        if port.manufacturer == "Arduino (www.arduino.cc)":
            return port


class SoilHumidity(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    potId: Optional[int] = None
    airLevel: Optional[int] = None
    waterLevel: Optional[int] = None
    soilHumidity: Optional[int] = None
    soilHumidityPercent: Optional[int] = None
    airTemperature: Optional[float] = None
    airHumidity: Optional[float] = None
    heatIndex: Optional[float] = None


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Humidity sensor")
    parser.add_argument("port", help="COM port to read", nargs="?")
    parser.add_argument("-b", "--baudrate", type=int, help="COM port baudrate", choices=Serial.BAUDRATES, default=19200)
    parser.add_argument("-l", "--list", action="store_true", help="list available COM ports and exit")
    parser.add_argument("-v", "--verbose", action="store_true", help="show more messages")
    parser.add_argument("-q", "--quiet", action="store_true", help="suppress all messages")

    args = parser.parse_args()

    if args.list:
        for port in comports():
            if args.verbose:
                print(f"""\
{port}
  device       : {port.device}
  name         : {port.name}
  description  : {port.description}
  hwid         : {port.hwid}
  vid          : {port.vid}
  pid          : {port.pid}
  serial_number: {port.serial_number}
  location     : {port.location}
  manufacturer : {port.manufacturer}
  product      : {port.product}
  interface    : {port.interface}
""")

            else:
                print(port)
        return

    if not args.port:
        port = suggest_comport()
        if port:
            args.port = suggest_comport().device

    if not args.port:
        print("Device not found")
        return

    port = Serial(
        port=args.port,
        baudrate=args.baudrate
    )

    with port:
        port.read_until(b'\x9a\x16\x52\x76\xa8\x1b')
        port.read(22)
        try:
            checkpoint = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
            columns = list(SoilHumidity.schema()["properties"].keys())
            while True:
                path = Path.home() / f".garden/metrics/pots/humidity/{datetime.utcnow():%Y-%m-%d/%H-%M-%S}.zip"
                # path = f"/tmp/{datetime.utcnow():%Y-%m-%d/%H-%M-%S}.zip"

                path.parent.mkdir(parents=True, exist_ok=True)
                if not args.quiet:
                    print(f"Writing to {path}")

                with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
                    with zf.open("metrics.csv", "w") as fp:
                        with io.TextIOWrapper(fp) as ffp:
                            writer = csv.DictWriter(ffp, fieldnames=columns)
                            writer.writeheader()
                            checkpoint += timedelta(hours=1)
                            while datetime.utcnow() < checkpoint:
                                # data = port.readline().decode("ascii").strip()
                                data = unpack("HHHHHHHHfff", port.read(28))
                                try:
                                    # data = SoilHumidity(**json.loads(data))
                                    data = SoilHumidity(**{k: v for k, v in zip(columns[1:], data[3:])})
                                    writer.writerow(data.dict())
                                    if args.verbose:
                                        print(data.json())
                                except json.JSONDecodeError:
                                    if not args.quiet:
                                        print("Invalid data:", data)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
