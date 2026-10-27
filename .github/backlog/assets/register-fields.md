# Register families, short names and kpi flags (architect, 27 Oct 2026)

Source for Tasks `data/ingestion/register-restructure/fields/*`. Copy the three values into each register entry as
written. Families A–K per program plan §7.1 and design §4.2. `kpi: true` = the source publishes aggregate statistics
that become catalogue KPIs; registers, geodata services, event feeds and unlicensed market/platform sources are false.

| id | family | short_name | kpi |
|---|---|---|---|
| GL-001 | A | Eurostat | true |
| GL-002 | A | Eurostat Comext | true |
| GL-003 | A | OECD Data Explorer | true |
| GL-004 | A | OECD PISA | true |
| GL-005 | A | World Bank WDI | true |
| GL-006 | A | World Bank GEM | true |
| GL-007 | A | World Bank IDS | true |
| GL-008 | A | World Bank WGI | true |
| GL-009 | A | World Bank Global Findex | true |
| GL-010 | A | World Bank Enterprise Surveys | true |
| GL-011 | A | World Bank ICP | true |
| GL-012 | A | IMF WEO | true |
| GL-013 | A | IMF IFS | true |
| GL-014 | A | IMF DOTS | true |
| GL-015 | C | BIS Data Portal | true |
| GL-016 | C | ECB Data Portal | true |
| GL-017 | A | UNdata | true |
| GL-018 | A | UN SDG Indicators | true |
| GL-019 | A | UN Comtrade | true |
| GL-020 | A | WTO Stats | true |
| GL-021 | A | UNCTAD Data Hub | true |
| GL-022 | A | FAOSTAT | true |
| GL-023 | A | ILOSTAT | true |
| GL-024 | A | WHO GHO | true |
| GL-025 | A | WHO data.who.int | true |
| GL-026 | A | UNESCO UIS | true |
| GL-027 | A | UNICEF Data Warehouse | true |
| GL-028 | A | UNDP Human Development | true |
| GL-029 | A | UNHCR Refugee Statistics | true |
| GL-030 | A | IOM Migration Data Portal | true |
| GL-031 | A | ITU DataHub | true |
| GL-032 | A | WIPO IP Statistics | true |
| GL-033 | A | UNODC Data Portal | true |
| GL-034 | G | IEA Data Explorer | true |
| GL-035 | G | IRENASTAT | true |
| GL-036 | G | Copernicus Climate Data Store | true |
| GL-037 | G | EEA Datahub | true |
| GL-038 | A | UNECE Data Portal | true |
| GL-039 | A | UN World Population Prospects | true |
| GL-040 | A | UNIDO INDSTAT | true |
| PLS-001 | B | GUS BDL | true |
| PLS-002 | B | GUS DBW | true |
| PLS-003 | B | GUS STRATEG | true |
| PLS-004 | B | GUS publication tables | true |
| PLS-005 | B | GUS NSP 2021 | true |
| PLS-006 | B | GUS PSR 2020 | true |
| PLS-007 | E | GUS TERYT | false |
| PLS-008 | E | GUS REGON BIR1 | false |
| PLS-009 | B | ZUS statistical portal | true |
| PLS-010 | B | ZUS social insurance yearbook | true |
| PLS-011 | B | KRUS statistics | true |
| PLS-012 | B | NFZ statistics: services | true |
| PLS-013 | B | NFZ statistics: pharmacy refunds | true |
| PLS-014 | B | NFZ contracts | true |
| PLS-015 | B | NFZ waiting times | true |
| PLS-016 | B | CeZ e-health in numbers | true |
| PLS-017 | B | MZ health statistics bulletin | true |
| PLS-018 | B | GIS sanitary state report | true |
| PLS-019 | B | NIZP-PZH epidemiology bulletins | true |
| PLS-020 | E | RPWDL healthcare providers | false |
| PLS-021 | E | RSPO schools register | false |
| PLS-022 | B | SIO education statistics | true |
| PLS-023 | B | RAD-on / POL-on | true |
| PLS-024 | B | ELA graduate outcomes | true |
| PLS-025 | B | CKE exam results | true |
| PLS-026 | E | NID heritage register | false |
| PLS-027 | B | GUS culture tables | true |
| PLS-028 | B | GUS cultural participation | true |
| PLS-029 | B | MRPiPS registered unemployment | true |
| PLS-030 | B | Barometr Zawodów | true |
| PLS-031 | B | MRPiPS social assistance | true |
| PLS-032 | B | MRiRW ZSRIR market information | true |
| PLS-033 | B | KOWR agricultural markets | true |
| PLS-034 | D | ARiMR payment statistics | true |
| PLS-035 | B | GUS tourism tables | true |
| PLG-001 | C | NBP exchange rates and gold | true |
| PLG-002 | C | NBP balance of payments | true |
| PLG-003 | C | NBP investment position | true |
| PLG-004 | C | NBP monetary aggregates | true |
| PLG-005 | C | NBP banking statistics | true |
| PLG-006 | C | NBP interest rates | true |
| PLG-007 | D | MF state budget | true |
| PLG-008 | D | MF Treasury debt | true |
| PLG-009 | D | MF public debt | true |
| PLG-010 | D | MF local-government budgets | true |
| PLG-011 | E | MF VAT white list | false |
| PLG-012 | E | KRS open API | false |
| PLG-013 | I | KRS financial documents (RDF) | false |
| PLG-014 | E | CEIDG | false |
| PLG-015 | E | CRBR beneficial owners | false |
| PLG-016 | E | KNF supervised entities | false |
| PLG-017 | C | KNF banking sector data | true |
| PLG-018 | C | KNF insurance and pensions data | true |
| PLG-019 | E | KNF public warning list | false |
| PLG-020 | F | UOKiK decisions | false |
| PLG-021 | F | UOKiK concentration decisions | false |
| PLG-022 | D | e-Zamówienia BZP | true |
| PLG-023 | D | TED notices (Poland) | true |
| PLG-024 | F | Sejm API | false |
| PLG-025 | F | Senate data | false |
| PLG-026 | F | ELI Dziennik Ustaw | false |
| PLG-027 | F | ISAP legal acts | false |
| PLG-028 | F | NIK audit results | false |
| PLG-029 | F | PKW election results | true |
| PLG-030 | J | dane.gov.pl | false |
| PLG-031 | F | ISWS justice statistics | true |
| PLG-032 | F | Prison Service statistics | true |
| PLG-033 | F | Police statistics | true |
| PLG-034 | F | RCL legislative process (RPL) | false |
| PL-ENV-001 | E | GUGiK PRG | false |
| PL-ENV-002 | E | GUGiK PRNG | false |
| PL-ENV-003 | G | GUGiK BDOT10k | false |
| PL-ENV-004 | G | GUGiK orthophoto | false |
| PL-ENV-005 | G | GUGiK elevation (NMT) | false |
| PL-ENV-006 | G | GUGiK KI EGiB | false |
| PL-ENV-007 | E | GUGiK address points | false |
| PL-ENV-008 | G | IMGW public data API | true |
| PL-ENV-009 | G | IMGW observation archive | true |
| PL-ENV-010 | G | GIOŚ air quality | true |
| PL-ENV-011 | G | GIOŚ surface water | true |
| PL-ENV-012 | G | GIOŚ groundwater | true |
| PL-ENV-013 | G | Wody Polskie Hydroportal | false |
| PL-ENV-014 | G | PIG-PIB boreholes | false |
| PL-ENV-015 | G | PIG-PIB MIDAS deposits | false |
| PL-ENV-016 | G | PIG-PIB hydrogeological map | false |
| PL-ENV-017 | G | GDOŚ nature protection | false |
| PL-ENV-018 | G | Lasy Państwowe forest data | false |
| PL-ENV-019 | G | PSE reports | true |
| PL-ENV-020 | G | URE energy prices | true |
| PL-ENV-021 | G | GAZ-SYSTEM transparency | true |
| PL-ENV-022 | H | GDDKiA traffic census (GPR) | true |
| PL-ENV-023 | H | GDDKiA road downloads | false |
| PL-ENV-024 | H | CEPiK | true |
| PL-ENV-025 | H | UTK rail data | true |
| PL-ENV-026 | H | ULC airport traffic | true |
| PL-ENV-027 | H | Port of Gdańsk statistics | true |
| PL-ENV-028 | H | Warsaw open data and GTFS | false |
| PL-ENV-029 | H | Gdańsk open data and GTFS | false |
| PL-ENV-030 | H | Poznań GTFS | false |
| PL-ENV-031 | H | Kraków GTFS | false |
| PL-ENV-032 | H | Wrocław transport open data | false |
| PL-ENV-033 | H | GUNB RWDZ building register | false |
| PL-ENV-034 | B | internet.gov.pl broadband | true |
| PL-ENV-035 | G | Rejestr Urbanistyczny | false |
| CD-001 | I | Marketstack | false |
| CD-002 | I | Alpha Vantage | false |
| CD-003 | I | Financial Modeling Prep | false |
| CD-004 | I | Nasdaq Data Link | false |
| CD-005 | I | GPW market data | false |
| CD-006 | I | KNF ESPI | false |
| CD-007 | I | GPW EBI | false |
| CD-008 | I | PAP Biznes ESPI/EBI | false |
| CD-009 | I | Stooq quotations | false |
| CD-010 | I | TGE market data | false |
| CD-011 | I | GPW Benchmark | false |
| CD-012 | I | SEC EDGAR | false |
| CD-013 | I | ESEF annual reports | false |
| CD-014 | I | Companies House | false |
| CD-015 | E | GLEIF | false |
| CD-016 | I | OpenCorporates | false |
| CD-017 | J | YouTube Data API | false |
| CD-018 | J | Meta Content Library | false |
| CD-019 | J | Instagram API | false |
| CD-020 | J | X API | false |
| CD-021 | J | LinkedIn API | false |
| CD-022 | J | Reddit Data API | false |
| CD-023 | J | TikTok Research API | false |
| CD-024 | J | TikTok Commercial Content API | false |
| CD-025 | J | Wikimedia | false |
| CD-026 | J | Wikidata | false |
| CD-027 | J | OpenStreetMap | false |
| CD-028 | J | OpenAlex | false |
| CD-029 | J | Crossref | false |
| CD-030 | J | GDELT | false |
| CD-031 | J | Our World in Data | true |
| CD-032 | J | Allegro API | false |
| CD-033 | J | Otodom reports | false |
| CD-034 | J | OTOMOTO market report | false |
| EX-001 | B | UPRP industrial-property statistics | true |
| EX-002 | E | UPRP e-Wyszukiwarka | false |
| EX-003 | F | PSP fire-service statistics | true |
| EX-004 | J | EPO Open Patent Services | false |
| EX-005 | E | OpenData organisations | false |
