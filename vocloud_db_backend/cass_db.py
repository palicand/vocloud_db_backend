from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import DoesNotExist
import uuid
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class CassSpectrum(Model):
    __table_name__ = "spectra"
    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    spectrum_id = columns.Text(index=True)
    wavelengths = columns.List(columns.Double())
    intensities = columns.List(columns.Double())
    metadata = columns.Map(key_type=columns.Text, value_type=columns.Text)

    def to_dict(self):
        return {
            "id": self.spectrum_id,
            "wavelengths": self.intensities,
            "intensities": self.intensities,
            "metadata": self.metadata
        }


def connect(hosts, port=9042):
    connection.setup(hosts, "vocloud", port=port)
    connection.session.execute("CREATE KEYSPACE IF NOT EXISTS vocloud WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};")
    sync_table(CassSpectrum)


def insert_spectrum(spectrum):
    spectrum_id = spectrum["id"]
    wavelengths = spectrum["wavelengths"]
    intensities = spectrum["intensities"]
    metadata = {key: str(value) for key, value in spectrum.get("metadata", {})}
    data = CassSpectrum.create(spectrum_id=spectrum_id, wavelengths=wavelengths,
                               intensities=intensities, metadata=metadata)
    return [instance.to_dict()
            for instance in CassSpectrum.objects(spectrum_id=spectrum_id)]


def get_spectrum(spectrum_id):
    try:
        data = CassSpectrum.get(spectrum_id=spectrum_id)
        return [instance.to_dict() for instance in data]
    except DoesNotExist as e:
        return None