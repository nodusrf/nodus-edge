"""Utility functions for Nodus Edge."""


def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Compute the Levenshtein (edit) distance between two strings.

    Returns the minimum number of single-character edits (insertions,
    deletions, substitutions) required to change s1 into s2.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    prev_row = list(range(len(s2) + 1))

    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            # Cost is 0 if characters match, 1 otherwise
            cost = 0 if c1 == c2 else 1
            curr_row.append(min(
                curr_row[j] + 1,        # insertion
                prev_row[j + 1] + 1,    # deletion
                prev_row[j] + cost,      # substitution
            ))
        prev_row = curr_row

    return prev_row[-1]
