"""Round-2 GEM probe entry (runs dev/w3intl_probe3.gem in its own concurrency group)."""

from dev.w3intl_probe3 import gem

if __name__ == "__main__":
    gem()
