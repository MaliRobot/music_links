from difflib import SequenceMatcher


def string_similarity_ratio(a, b):
    return SequenceMatcher(None, a, b).ratio()
