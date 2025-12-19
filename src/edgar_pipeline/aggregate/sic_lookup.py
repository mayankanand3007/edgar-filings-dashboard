"""Utility: small static CIK → SIC mapping used for optional enrichment.

This module is intentionally lightweight; for production use you'd replace
this with a lookup backed by a dataset or external API.
"""

# Simple static mapping

CIK_TO_SIC = {
    895421: "6211",   # Morgan Stanley
    310158: "6021",   # JPMorgan Chase
    1403161: "7372",  # Alphabet
    # add more as needed
}

def get_sic(cik: int) -> str | None:
    """Return the SIC code for a given CIK if available.

    Args:
        cik: Central Index Key (integer).

    Returns:
        SIC code as string or ``None`` when unknown.
    """
    return CIK_TO_SIC.get(cik)