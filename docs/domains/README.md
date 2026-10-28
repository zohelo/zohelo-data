# Domain knowledge files

One file per domain, `docs/domains/<domain>.md`, written and kept current by the Domain expert (`docs/team.md`).
Domain keys follow `warehouse/seeds/domain.csv`. A domain becomes active when it has a production source with KPIs;
the expert creates its file on the first run for that domain.

| Domain | Key | Active | Main sources today |
|---|---|---|---|
| Economy | `economy` | yes | Eurostat, World Bank WDI, GUS BDL, GUS DBW |
| Finance | `finance` | yes | NBP, Eurostat |
| Business & industry | `business` | yes | GUS BDL, GUS DBW, GLEIF, MF biała lista |
| Labour & income | `labour` | yes | GUS BDL, Eurostat, World Bank WDI |
| Population | `population` | yes | GUS BDL, Eurostat, World Bank WDI |
| Housing & territory | `housing` | yes | GUS BDL, GUS TERYT, GUGiK PRG |
| Health | `health` | no | |
| Education & science | `education` | no | |
| Society & culture | `society` | no | |
| Environment & energy | `environment` | no | |
| Transport & digital | `transport` | no | |
| State, law & security | `state` | no | |

## Template

Every domain file has these sections, in this order, and stays under about 15 KB:

1. **Scope** — what the domain covers here, its `domain.csv` groups and subgroups.
2. **Key indicators** — the indicators professionals watch, each with definition, unit, frequency, the source and
   table we use, and how to read it (what a rise or fall means, normal ranges).
3. **Methodology traps** — real vs nominal, seasonal adjustment, base years and rebasing, breaks in series, revisions,
   differences between GUS, Eurostat and international definitions.
4. **Standard analyses** — the analyses and charts a practitioner expects (e.g. year-on-year, contributions, regional
   comparison, per capita), with the KPIs they need.
5. **Release calendar** — when each source publishes, so pages say how fresh a number is.
6. **Audience and questions** — who uses this domain's data and the questions they ask.
7. **Open points** — what is still unclear, with links to issues.

Facts come with a link to the primary source (statistical office, central bank, methodology note).
