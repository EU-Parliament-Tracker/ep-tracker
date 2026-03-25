---
layout: default
title: About EP Tracker
permalink: /about/
---

## What is EP Tracker?

EP Tracker is an independent, open-source tool for monitoring the activity
of the **European Parliament**. It is designed for EU policy professionals,
journalists, NGOs, and engaged citizens who want easy access to parliamentary
data without having to navigate the official EP website.

## What data is shown?

All data comes from the
[European Parliament Open Data Portal](https://data.europarl.europa.eu)
(API v2), which is the official open data service of the European Parliament.

The following datasets are updated daily:

- **MEPs** — all 720 sitting members of the 10th parliamentary term,
  including contact details, committee memberships, and social media links
- **Roll-call votes** — results of all recorded votes in plenary,
  including breakdown of for / against / abstentions
- **Plenary sessions** — past and upcoming sessions with links to
  agendas and minutes
- **Legislative documents** — adopted texts and documents tabled
  for plenary
- **Parliamentary questions** — written and oral questions submitted
  by MEPs
- **Committees** — all standing committees and their membership

## How is data updated?

A Python script runs daily via GitHub Actions, fetches the latest data
from the EP API, and commits updated JSON files to this repository.
Jekyll then rebuilds the static site automatically. No database or server
is required.

## Source code

This project is fully open source. You can inspect, fork, and contribute at:
[github.com/your-org/ep-tracker](https://github.com/your-org/ep-tracker)

## Data sources & attribution

Data is sourced from the European Parliament under their
[open data terms of use](https://data.europarl.europa.eu/en/developer-corner/opendata-api).
EP Tracker is an independent project and is not affiliated with or endorsed
by the European Parliament.

## Contact

Questions or suggestions? Open an issue on GitHub or email
[hello@example.com](mailto:hello@example.com).
