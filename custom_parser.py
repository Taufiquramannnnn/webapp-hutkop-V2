"""
CustomFieldParser untuk dbfread:
- DBF “kotor”: null byte, spasi, koma desimal (format lokal).
- Parser ini bikin pembacaan kolom numerik lebih toleran, supaya pipeline lancar.
"""

from dbfread import FieldParser
import logging

logger = logging.getLogger(__name__)

class CustomFieldParser(FieldParser):
    def parseN(self, field, data):
        """
        Override parser numerik:
        - buang null byte & trim
        - kosong -> None
        - coba int, kalau gagal -> float dengan koma→titik
        - kalau tetap gagal, log lalu fallback 0 (biar aplikasi ngga error)
        """
        try:
            data = data.replace(b"\x00", b"").strip()
            if data == b"":
                return None
            try:
                return int(data)
            except ValueError:
                try:
                    s = data.decode("latin1").replace(",", ".")
                    return float(s)
                except (ValueError, UnicodeDecodeError):
                    logger.warning(f"Gagal parsing numerik: {data}. fallback=0")
                    return 0
        except Exception as e:
            logger.error(f"parseN error: {e}")
            return 0
