from typing import List, Generator, Any


def chunk_data(data: List[Any], chunk_size: int) -> Generator[List[Any], None, None]:
    """Yields chunks of data."""
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]
