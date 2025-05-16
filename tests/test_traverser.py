import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.orm import Session

from services.traverser import StepTraverser, Traverser
from schemas.artist import ArtistCreate
from schemas.release import ReleaseCreate


@pytest.fixture
def mock_client():
    client = MagicMock()
    return client


@pytest.fixture
def mock_db():
    return MagicMock(spec=Session)


@pytest.fixture
def sample_artist_discogs():
    mock_artist = MagicMock()
    mock_artist.id = '123'
    mock_artist.name = 'Test Artist'
    mock_artist.url = 'http://discogs.com/artist/123'
    mock_artist.releases = [1,2,3]
    mock_release = MagicMock()
    mock_release.id = 'r1'
    mock_release.title = 'Release Title'
    mock_release.url = 'http://discogs.com/release/r1'
    mock_release.year = 2000
    mock_artist.releases = [mock_release]
    return mock_artist


def test_steptraverser_get_or_create_artist_creates_new(mock_client, mock_db, sample_artist_discogs):
    mock_client.fetch_artist_by_discogs_id.return_value = sample_artist_discogs

    with patch('crud.artist.artist_crud.get_by_discogs_id', return_value=None), \
            patch('crud.artist.artist_crud.create_with_releases') as mock_create:
        traverser = StepTraverser(
            discogs_id='123',
            client=mock_client,
            db=mock_db
        )
        traverser.get_or_create_artist()

        mock_client.fetch_artist_by_discogs_id.assert_called_with('123')
        mock_create.assert_called_once()


def test_steptraverser_get_or_create_artist_reuses_existing(mock_client, mock_db):
    artist = MagicMock()
    with patch('crud.artist.artist_crud.get_by_discogs_id', return_value=artist):
        traverser = StepTraverser(
            discogs_id='123',
            client=mock_client,
            db=mock_db
        )
        result = traverser.get_or_create_artist()
        assert result == artist


def test_traverser_traverse_loop_stops_after_limit(mock_client, mock_db, sample_artist_discogs):
    sample_artist_discogs.releases = []

    with patch('crud.artist.artist_crud.get_by_discogs_id', return_value=None), \
            patch('crud.artist.artist_crud.create_with_releases', return_value=MagicMock(discogs_id='123')), \
            patch('crud.release.release_crud.get_by_discogs_id', return_value=None), \
            patch('crud.release.release_crud.create'), \
            patch('crud.artist.artist_crud.add_artist_release'), \
            patch.object(mock_client, 'fetch_artist_by_discogs_id', return_value=sample_artist_discogs), \
            patch.object(mock_client, 'get_artist', return_value=sample_artist_discogs):
        traverser = Traverser(
            discogs_id='123',
            client=mock_client,
            db=mock_db,
            max_artists=1
        )
        traverser.begin_traverse()
        assert traverser.count == 1
