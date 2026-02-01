# Test Fixtures

This directory contains sample Druid segments for integration testing.

## wikipedia-segment

A real Druid segment from the Apache Druid Wikipedia quickstart dataset. This segment contains
Wikipedia edit events with the following schema:

### Dimension Columns (STRING type)
- `channel` - Wikipedia channel (e.g., `#en.wikipedia`)
- `cityName` - Editor's city
- `comment` - Edit comment
- `countryIsoCode` - ISO country code
- `countryName` - Country name
- `isAnonymous` - Whether the edit was anonymous
- `isMinor` - Whether it was a minor edit
- `isNew` - Whether this created a new page
- `isRobot` - Whether the edit was by a bot
- `isUnpatrolled` - Whether the edit was unpatrolled
- `metroCode` - Metro code
- `namespace` - Wikipedia namespace
- `page` - Page name
- `regionIsoCode` - ISO region code
- `regionName` - Region name
- `user` - Editor username

### Metric Columns (LONG type)
- `added` - Bytes added
- `deleted` - Bytes deleted
- `delta` - Net change in bytes

### Time Column
- `__time` - Event timestamp (LONG, milliseconds since epoch)

### Segment Info
- **Time range**: 2015-09-11T23:08:00.000Z to 2015-09-12T23:00:00.000Z
- **Rows**: ~24,000 rows
- **Version**: V9 segment format

### Files
- `00000.smoosh` - Main data file containing all columns
- `meta.smoosh` - Index mapping logical files to byte ranges in the smoosh file
- `version.bin` - Segment format version (V9)
- `factory.json` - Segment metadata factory info

### Source
Generated from the [Apache Druid quickstart tutorial](https://druid.apache.org/docs/latest/tutorials/).
